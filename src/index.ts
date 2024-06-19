import { stderr, stdout } from "node:process";
import { $ as build$ } from "execa";
import fg from "fast-glob";
import { createLogUpdate } from "log-update";
import stripAnsi from "strip-ansi";
import * as ansiColors from "yoctocolors";

type SimpleItem = string | number;

type GlobTemplate = SimpleItem | SimpleItem[] | { glob: string | string[] };
type Executor = typeof build$ & {
  (
    ...templateString: readonly [
      TemplateStringsArray,
      ...(readonly GlobTemplate[])
    ]
  ): ReturnType<typeof build$>;
};

export type Worker<T = any> = {
  readonly data: T;
  readonly displayTitle: (text: string) => void;
  readonly displayTitleTag: (tag: string) => void;
  readonly displayAnnotation: (text: string | undefined) => void;
  readonly $: Executor;
};

export type Task<T = any> = (this: Worker<T>, $: Executor) => Promise<unknown>;
export type Tasks<T = any> = Record<string, Task<T>>;

type TaskOptions<Term extends string> = {
  verbose?: boolean;
  data?: Partial<Record<Term, unknown>>;
};

export type Driver<Term extends string> = {
  run: (options?: TaskOptions<Term>) => Promise<void>;
};

type Configurator<Term extends string> = (
  when: Matcher<Term>,
  make: (key: Term) => Output<Term>
) => void;

type Matcher<Term extends string> = ((
  condition: Pattern<Term> | Pattern<Term>[] | null
) => Actions & Restrictions<Term>) &
  ((
    condition: Pattern<Term> | Pattern<Term>[] | null,
    output: Output<Term>
  ) => Actions & Restrictions<Term>);

type Actions = {
  readonly call: ((task: (executor: Executor) => Promise<unknown>) => void) &
    ((label: string, task: (executor: Executor) => Promise<unknown>) => void);
};

type Restrictions<Term extends string> = {
  readonly only: (options?: TaskOptions<Term>) => Actions;
};

type Pattern<Term extends string> = Term | `?${Term}` | `!${Term}`;
type Output<Term> = { make: Term };

type Step = {
  stepId: number;
  title: string;
  isQualified: (
    config: Record<string, unknown>,
    allowUndefined?: boolean
  ) => boolean;
  input: string[];
  output?: Output<string>;
  task: (executor: Executor) => unknown;
  data?: Record<string, unknown>;
};

type Runtime = {
  start: number;
  isInteractive: boolean;
  status: "active" | "failed" | "cancelled";
  exitCause?: { error?: unknown; interrupt?: string };
};

type Group = {
  data: Record<string, unknown>;
  activate: (output: Output<string>, result: unknown) => void;
  runtime: Runtime;
};

const colorLess = Object.fromEntries(
  Object.keys(ansiColors).map((k) => [k, identity])
) as typeof ansiColors;

const allowsInput = typeof process.stdin.setRawMode === "function";
const color = allowsInput ? ansiColors : colorLess;

const exec$ = withGlob(build$);

export function schedule<
  Source = string,
  Term extends string = Source extends string ? Source : keyof Source & string
>(define: Configurator<Term>): Driver<Term> {
  return {
    run: async (options = {}) => {
      let isInteractive = allowsInput;
      if (allowsInput) {
        const cursor = await readCursorPosition(100);
        if (cursor) {
          isInteractive = true;
          if (cursor.x > 1) {
            process.stderr.write("\n");
          }
        }
      }

      const runtime: Runtime = {
        start: Date.now(),
        status: "active",
        isInteractive,
      };

      // allows defining tasks container after schedule function
      await Promise.resolve();

      const steps: Step[] = [];
      let execution: "full" | "focused" = "full";
      let verboseOverride: boolean | undefined = undefined;
      const when: Matcher<Term> = (
        condition: Pattern<Term> | Pattern<Term>[] | null,
        output?: Output<Term>
      ) => {
        let focusedOptions: TaskOptions<Term> | undefined = undefined;
        const actions = {
          call: (
            nameSource: ((executor: Executor) => unknown) | string,
            fn?: (executor: Executor) => unknown
          ) => {
            const stepId = steps.length;
            const task =
              typeof nameSource === "function"
                ? nameSource
                : (nonNull(nameSource, "First argument is required, found %s"),
                  nonNull(
                    fn,
                    "Second argument function is required, found %s"
                  ));
            const title = color.bold(
              task !== nameSource
                ? String(nameSource)
                : getTitle(task, "#" + (steps.length + 1))
            );
            const input: string[] = [];
            const requirements: { term: string; expect: boolean }[] = [];
            if (!focusedOptions) {
              asList(condition).forEach((pattern) => {
                if (pattern.startsWith("!") || pattern.startsWith("?")) {
                  const term = pattern.slice(1);
                  input.push(validateTerm(term));
                  requirements.push({ term, expect: pattern.startsWith("?") });
                } else {
                  input.push(validateTerm(pattern));
                }
              });
            } else if (execution === "focused") {
              input.push(String(steps.length));
            }
            validateTerm(output?.make);
            const isQualified = (
              state: Record<string, unknown>,
              allowUndefined?: boolean
            ) =>
              (focusedOptions ? [] : requirements).every(
                (condition) =>
                  (allowUndefined && state[condition.term] === undefined) ||
                  !!state[condition.term] === condition.expect
              );
            if (focusedOptions) {
              if (execution === "full") {
                steps.length = 0;
              }
              execution = "focused";
            }
            if (verboseOverride === undefined && focusedOptions) {
              if (typeof focusedOptions.verbose === "boolean") {
                verboseOverride = focusedOptions.verbose;
              }
            }
            if (
              execution === "full" ||
              (execution === "focused" && focusedOptions)
            ) {
              steps.push({
                stepId,
                title,
                isQualified,
                input: input,
                output: focusedOptions
                  ? { make: String(steps.length + 1) }
                  : output,
                task,
                data: focusedOptions?.data,
              });
            }
          },
        };

        return {
          ...actions,
          only: (options?: TaskOptions<Term>) => {
            focusedOptions = options ?? {};
            return actions;
          },
        };
      };

      const make = (make: Term) => ({ make });
      define(when, make);

      const config = options.data ?? ({} as Record<Term, unknown>);
      const runnable = steps.filter((it) => it.isQualified(config, true));
      const inputs = new Set(runnable.flatMap((step) => step.input));
      const done = new Set(
        [...inputs].filter((key) => config && config[key as Term] !== undefined)
      );
      const isReady = (step: Step) => step.input.every((key) => done.has(key));
      for (const key of inputs) {
        verifyExecutionPathExists(runnable, key, [], done, isReady);
      }

      const verbose =
        verboseOverride ??
        (options.verbose != null
          ? !!options.verbose
          : process.argv.includes("--verbose") || !isInteractive);

      const remaining = new Set(runnable);

      const ready = runnable.filter(isReady);
      ready.forEach((step) => remaining.delete(step));

      const reporter = new TaskReporter(runtime, verbose);

      if (verbose && runnable.length < steps.length) {
        for (const skipped of steps) {
          if (!runnable.includes(skipped)) {
            reporter.logger?.log("SKIPPING", skipped.title, color.gray("@00"));
          }
        }
      }

      const group: Group = {
        data: { ...config },
        runtime,
        activate(output, result: unknown) {
          group.data[output.make] = result;
          done.add(output.make);
          const ready: Step[] = [];
          for (const step of remaining) {
            if (step.input.every((key) => done.has(key))) {
              remaining.delete(step);
              ready.push(step);
              if (step.data) {
                Object.assign(group.data, step.data);
              }
            }
          }
          ready.forEach((step) => reporter.start(this, step));
        },
      };

      const cancelTasks = () => {
        if (runtime.status === "active") {
          runtime.status = "cancelled";
          runtime.exitCause = { interrupt: "SIGINT" };
        } else if (
          runtime.status === "failed" ||
          runtime.status === "cancelled"
        ) {
          process.exit(1);
        }
      };
      process.on("SIGINT", cancelTasks);

      ready.forEach((step) => reporter.start(group, step));

      try {
        await reporter.result();
      } finally {
        process.off("SIGINT", cancelTasks);
      }
    },
  };
}

type TaskTracker = {
  title: string;
  titleTag?: string;
  step: Step;
  marker: string;
  annotation?: string;
};

const ID_ALT = ((all: string) => [
  ...all.split(""),
  ...all.toUpperCase().split(""),
])("abcdefghijklmnopqrstuvwxyz");

function generateIdLabel(value: number): string {
  if (value < 100) {
    return String(value).padStart(2, "0");
  } else {
    const last = (value - 100) % ID_ALT.length;
    const first = (value - 100 - last) / ID_ALT.length;
    return first < ID_ALT.length ? ID_ALT[first] + ID_ALT[last] : "??";
  }
}

class TaskReporter {
  runtime: Runtime;
  running: TaskTracker[] = [];
  nextId = 1;
  logger: Logger | undefined;
  timer: NodeJS.Timeout | undefined;
  puffer: ReturnType<typeof startPuffer> | undefined;
  // @ts-ignore
  resolve: () => void;
  // @ts-ignore
  reject: (e: unknown) => void;
  outcome = new Promise<void>(
    (res, rej) => ((this.resolve = res), (this.reject = rej))
  );

  constructor(runtime: Runtime, verbose: boolean) {
    this.runtime = runtime;
    if (verbose) {
      this.logger = new Logger();
    } else {
      this.puffer = startPuffer(this.renderTaskList);
      this.timer = setInterval(() => this.puffer?.refresh(), 120);
    }
  }

  issueId() {
    const result = `@${generateIdLabel(this.nextId)}`;
    this.nextId += 1;
    return result;
  }

  symbols = {
    done: color.green("\u2714"),
    fail: color.red("\u2716"),
    stop: color.red("\u25FC"),
  };

  start(group: Group, step: Step) {
    const wid = color.cyan(this.issueId());
    const tracker: TaskTracker = {
      step,
      title: step.title,
      marker: wid,
    };
    const stepStart = Date.now();
    const executor = (...args: Parameters<Executor>) => {
      const x = exec$(...args);
      if (Symbol.asyncIterator in x) {
        this.logger?.log("COMMAND", `${x.spawnargs.join(" ")}`, wid);
        const commandAnnotation =
          color.bold(color.cyanBright("$ ")) + x.spawnargs.join(" ");
        tracker.annotation = commandAnnotation;
        if (this.logger) {
          (async () => {
            for await (const line of x) {
              this.logger?.log("OUTPUT", String(line), wid);
            }
          })().catch(() => "ignore");
        }
        const clearAnnotation = () => {
          if (tracker.annotation === commandAnnotation) {
            tracker.annotation = undefined;
          }
        };
        x.then(clearAnnotation).catch(clearAnnotation);
      }
      return x;
    };
    const worker: Worker = {
      data: group.data,
      displayTitle: (title: string) =>
        void (tracker.title = color.bold(stripAnsi(title))),
      displayTitleTag: (tag: string) =>
        void (tracker.titleTag = stripAnsi(tag)),
      displayAnnotation: (text: string | undefined) =>
        void (tracker.annotation = text ? stripAnsi(text) : text),
      $: executor as unknown as Executor,
    };
    this.running.push(tracker);
    this.logger?.log("STARTING", step.title, wid);
    Promise.resolve(step.task.call(worker, executor as unknown as Executor))
      .then((result) => {
        const styled = this.logger ? color.green : color.dim;
        const duration = styled(formatDuration(Date.now() - stepStart));
        this.logger?.log("FINISHED", `${step.title} ${duration}`, wid);
        this.puffer?.emit(`${this.symbols.done} ${step.title} ${duration}\n`);
        if (step.output && this.runtime.status === "active") {
          group.activate(step.output, result);
        }
      })
      .catch((error) => {
        const wasCancelled =
          this.runtime.exitCause?.interrupt &&
          error?.signal === this.runtime.exitCause.interrupt;
        if (wasCancelled) {
          this.puffer?.emit(`${this.symbols.stop} ${step.title}\n`);
          if (this.logger && this.runtime.isInteractive) {
            process.stdout.write("\n");
          }
          this.logger?.log("CANCELLED", step.title, wid);
        } else {
          this.puffer?.emit(`${this.symbols.fail} ${color.red(step.title)}\n`);
          this.logger?.log("FAILED", color.red(step.title) + "\n" + error, wid);
        }
        if (!this.runtime.exitCause) {
          this.runtime.exitCause = { error };
          this.runtime.status = "failed";
        }
      })
      .finally(() => {
        const pos = this.running.indexOf(tracker);
        this.running.splice(pos, 1);
        if (this.running.length === 0) {
          this.puffer?.stop();
          clearInterval(this.timer);
          this.timer = undefined;
          const duration = formatDuration(Date.now() - this.runtime.start);
          switch (group.runtime.status) {
            case "active":
              this.logger?.log("DONE", `Done in ${duration}`);
              this.resolve();
              break;
            case "failed":
              const failure = group.runtime.exitCause;
              const error =
                failure && "error" in failure ? failure.error : undefined;
              this.reject(error);
              break;
            case "cancelled":
              const info = group.runtime.exitCause?.interrupt
                ? ` by ${group.runtime.exitCause?.interrupt}`
                : "";
              this.logger?.log("CANCELLED", `Cancelled${info}`, wid);
              break;
          }
        }
      });
  }

  result() {
    return this.outcome;
  }

  active = 0;
  frames = ["○", "◎", "◉", "●", "◉", "◎", "○", "◯"];
  renderTaskList = () => {
    const l = this.frames.length;
    const x = (this.active = this.active + 1);
    const dot = this.runtime.status === "active" ? color.cyanBright : color.red;
    let info = this.running
      .map((tracker, i) => {
        const frame = dot(this.frames[(x + ((l - i) % l)) % l]);
        const tag = tracker.titleTag ? " " + tracker.titleTag : "";
        let line = `${frame} ${tracker.title}${tag}\n`;
        if (tracker.annotation) {
          const short = tracker.annotation.slice(0, (stdout.columns ?? 80) - 5);
          line += `  ${short}\n`;
        }
        return line;
      })
      .join("");

    if (
      this.runtime.status === "failed" ||
      this.runtime.status === "cancelled"
    ) {
      if (this.running.length > 0) {
        const status =
          this.runtime.status === "cancelled" ? "Cancelled" : "Failed";
        const leftovers =
          this.running.length === 1 ? "1 task" : this.running.length + " tasks";
        info += `\n${status}, finishing ${leftovers} (Press Ctrl+C to exit)`;
      } else {
        if (!this.logger && this.runtime.exitCause?.interrupt) {
          info += `\nCancelled by ${this.runtime.exitCause?.interrupt}`;
        }
      }
    } else {
      info += //
        color.dim(`\nTime: ${formatDuration(Date.now() - this.runtime.start)}`);
    }
    return info;
  };
}

type LoggerAction =
  | ("STARTING" | "FINISHED" | "FAILED" | "CANCELLED")
  | ("SKIPPING" | "DONE" | "COMMAND" | "OUTPUT");
class Logger {
  stream = process.stdout;
  tagColors: Partial<Record<LoggerAction, typeof color.dim>> = {
    STARTING: color.yellow,
    FINISHED: color.green,
    SKIPPING: color.gray,
    FAILED: color.red,
    CANCELLED: color.red,
  };
  messageColors: Partial<Record<LoggerAction, typeof color.dim>> = {
    STARTING: color.gray,
    SKIPPING: color.dim,
  };
  symbols: Partial<Record<LoggerAction, string>> = {
    COMMAND: "$",
    OUTPUT: "\u203A",
    DONE: "",
  };
  log(action: LoggerAction, message: string, stepMarker = "") {
    const ts = getFormattedTimestamp();
    const paint = this.tagColors[action] ?? identity;
    const print = this.messageColors[action] ?? identity;
    const symbol = this.symbols[action] ?? "~";
    const id = stepMarker || symbol ? ` ${stepMarker}${symbol}` : "";
    const prefix = `${color.dim(`[${ts}]`)}${id}`;
    const annotation =
      action === "OUTPUT" || action === "COMMAND" || action === "DONE"
        ? ""
        : paint(`[${action}] `);
    this.stream.write(`${prefix} ${annotation}${print(message)}\n`);
  }
}

function sanitizeText(text: string): string {
  // TODO: leave colors when stripping ansi, reveal other ansi codes as plain text
  // https://github.com/netzkolchose/node-ansiparser
  return stripAnsi(text);
}

function startPuffer(getState: () => string) {
  const stream = process.stderr;
  const stdoutWrite = process.stdout.write;
  const stderrWrite = process.stderr.write;
  const logUpdate = createLogUpdate(stream, { showCursor: true });
  let buffer = "";
  let justUpdateBuffer = false;
  let prefix = "";
  const getOutput = () => {
    const state = getState();
    return prefix === "" ? state : prefix + "\n" + state;
  };
  const puffer = {
    write: (data: unknown) => {
      return puffer.emit(data, sanitizeText);
    },
    emit: (data: unknown, clean: typeof sanitizeText = identity) => {
      const text = String(data);
      if (!justUpdateBuffer) {
        justUpdateBuffer = true;
        logUpdate.clear();
        const debris = clean(text);
        const lines = debris.split(/\r?\n/);
        if (lines.length === 1) {
          prefix += lines[0];
        } else {
          buffer += prefix + debris;
          prefix = String(lines.at(-1));
        }
        logUpdate(getOutput());
        const result = stdoutWrite.call(stream, buffer);
        buffer = "";
        justUpdateBuffer = false;
        return result;
      } else {
        buffer += text;
        return false;
      }
    },
    refresh: () => {
      justUpdateBuffer = true;
      logUpdate(getOutput());
      stdoutWrite.call(stream, buffer);
      buffer = "";
      justUpdateBuffer = false;
    },
    stop: () => {
      puffer.refresh();
      logUpdate.done();
      buffer = "";
      prefix = "";
      stdout.write = stdoutWrite;
      stderr.write = stderrWrite;
    },
  };
  stdout.write = puffer.write;
  stderr.write = puffer.write;
  return puffer;
}

function withGlob(original: typeof build$, cwd?: string): Executor {
  return Object.assign((...args: Parameters<Executor>) => {
    if (Array.isArray(args[0]) && "raw" in args[0]) {
      args.forEach((x, index) => {
        if (typeof x === "object" && x != null && "glob" in x) {
          const wildcard = x.glob;
          if (
            typeof wildcard === "string" ||
            (Array.isArray(wildcard) &&
              wildcard.every((it: unknown) => typeof it === "string"))
          ) {
            const matches = fg.sync(wildcard, { cwd });
            if (matches.length === 0) {
              const at = cwd ? ` in directory ${cwd}` : "";
              throw new Error(`No matches for wildcard '${wildcard}'${at}`);
            }
            args[index] = matches as any;
          }
        }
      });
      return (original as Function).apply(null, args);
    } else {
      const output = (original as Function).apply(null, args);
      if (typeof output === "function") {
        return withGlob(output, (args[0] as any)?.cwd ?? cwd);
      }
    }
  }, original);
}

export function glob(strings: TemplateStringsArray, ...values: unknown[]) {
  return { glob: String.raw(strings, ...values) };
}

function verifyExecutionPathExists(
  steps: Step[],
  key: string,
  visited: Step[],
  done: Set<string>,
  isRoot: (step: Step) => boolean
): void {
  const preceding = steps.filter((t) => t.output?.make === key);
  if (preceding.length === 0 && !done.has(key)) {
    const origin = steps.find((step) => step.input.includes(key));
    const info = origin ? ` required for ${origin.title}` : "";
    throw new Error(`Cannot find how to make "${key}"${info}`);
  }
  const remaining = preceding.filter((t) => !isRoot(t));
  for (const option of remaining) {
    if (visited.includes(option)) {
      const cycle = [...visited, option].map((it) => it.title).join("->");
      throw new Error(`Unsupported cycle found "${cycle}"`);
    }
    const optionPath = visited.concat(option);
    for (const target of option.input) {
      verifyExecutionPathExists(steps, target, optionPath, done, isRoot);
    }
  }
}

function identity<T>(x: T): T {
  return x;
}

function nonNull<T>(value: T | null | undefined, message: string): T {
  if (value == null) {
    throw new Error(message.replace("%s", String(value)));
  }
  return value;
}

function asList<T>(value: T | T[] | null): T[] {
  return value === null ? [] : Array.isArray(value) ? value : [value];
}

function getFormattedTimestamp() {
  return formatTimestamp(new Date());
}

function formatTimestamp(time: Date) {
  return (
    String(time.getHours()).padStart(2, "0") +
    ":" +
    String(time.getMinutes()).padStart(2, "0") +
    ":" +
    String(time.getSeconds()).padStart(2, "0") +
    "." +
    String(time.getMilliseconds()).padStart(3, "0")
  );
}

function formatDuration(millis: number) {
  if (millis < 100000) {
    return (millis / 1000).toFixed(1) + "s";
  }
  let minutes = Math.floor(millis / 60000);
  let seconds = ((millis - minutes * 60000) / 1000).toFixed(0);
  if (seconds === "60") {
    minutes += 1;
    seconds = "00";
  }
  return `${minutes.toFixed(0)}m${seconds.padStart(2, "0")}s`;
}

function parsePosition(encoded: string) {
  for (let c = 0, x = "", y = "", o = ""; c < encoded.length; c++) {
    const char = encoded[c];
    switch (char) {
      case "[":
        o = "y";
        break;
      case ";":
        o = "x";
        break;
      case "R":
        if (x !== "" && y !== "") {
          const result = { x: Number(x), y: Number(y) };
          return isNaN(result.x) || isNaN(result.y) ? null : result;
        }
        return null;
      default: {
        o === "x" && (x += char);
        o === "y" && (y += char);
        break;
      }
    }
  }
  return null;
}

function readCursorPosition(timeout = 80) {
  if (typeof process.stdin.setRawMode !== "function") {
    return Promise.resolve(null);
  }
  let isResolved = false;
  return new Promise<{ x: number; y: number } | null>((resolve) => {
    const initiallyRaw = process.stdin.isRaw;
    !initiallyRaw && process.stdin.setRawMode(true);
    const finish = (result: boolean) => {
      if (!isResolved) {
        isResolved = true;
        const data = result && process.stdin.read();
        !initiallyRaw && process.stdin.setRawMode(false);
        resolve(result ? parsePosition(String(data)) : null);
      }
    };
    const acceptResponse = () => finish(true);
    process.stdin.once("readable", acceptResponse);
    process.stdout.write("\u001b[6n");
    setTimeout(() => {
      process.stdin.off("readable", acceptResponse);
      finish(false);
    }, timeout);
  });
}

const ReservedSymbol = /(\?|\!|\*|\#|\^|\:\(\))/;
function validateTerm<T>(value: T): T {
  if (typeof value === "string") {
    const res = ReservedSymbol.exec(value);
    if (res) {
      throw new Error(`Reserved symbol ${res} found in ${value}`);
    }
  }
  return value;
}

function getTitle(fn: Function, alternative: string) {
  if (typeof fn.name !== "string" || fn.name.includes("=>")) {
    return alternative;
  } else {
    return rephrase(fn.name);
  }
}

function rephrase(original: string): string {
  const withSpacing = original.includes(" ")
    ? original
    : original.includes("_")
    ? original.replace(/_/g, " ")
    : original
        .replace(/[A-Z][a-z]/g, (m) => " " + m.toLowerCase())
        .replace(/[A-Z][A-Z0-9]+/g, (m) => " " + m)
        .replace(/[^\sa-zA-Z]+(\s|$)/g, (m) =>
          /^[0-9]+$/.test(m) ? " " + m : m
        );
  return withSpacing.trim().replace(/^./, (m) => m.toUpperCase());
}

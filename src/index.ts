import { stderr, stdout } from "node:process";
import fs from "node:fs/promises";
import crypto from "node:crypto";
import { $ as build$ } from "execa";
import fg from "fast-glob";
import { createLogUpdate } from "log-update";
import stripAnsi from "strip-ansi";
import * as ansiColors from "yoctocolors";

type SimpleItem = string | number;

type GlobItem = SimpleItem | SimpleItem[] | { glob: string | string[] };
type ExecWithGlob<T = ReturnType<typeof build$>> = (
  ...templateString: readonly [TemplateStringsArray, ...(readonly GlobItem[])]
) => T;
type GlobExecutor = typeof build$ & ExecWithGlob;
type Executor = GlobExecutor & {
  read: ExecWithGlob<Promise<string>> &
    ((stream: "stdout" | "stderr") => ExecWithGlob<Promise<string>>);
};

export type Worker<T = any> = {
  readonly data: T;
  readonly displayTitle: (text: string) => void;
  readonly displayTitleTag: (tag: string) => void;
  readonly addActivity: (activity: string) => UpdateActivity;
  readonly $: Executor;
};

export type Task<T = any> = (this: Worker<T>, $: Executor) => Promise<unknown>;
export type Tasks<T = any> = Record<string, Task<T>>;

type TaskOptions<Token extends string> = {
  verbose?: boolean;
  data?: Partial<Record<Token, unknown>>;
};

type RunOptions<Token extends string> = TaskOptions<Token> & {
  skip?: "never" | "allow";
};

export type Driver<Token extends string> = {
  run: (options?: TaskOptions<Token>) => Promise<void>;
};

export type UpdateActivity = (result?: unknown) => void;

type Configurator<Token extends string> = (
  when: Matcher<Token>,
  make: (key: Token) => Output<Token>
) => void;

type Matcher<Token extends string> = ((
  trigger: Pattern<Token> | Pattern<Token>[] | null
) => Actions & Conditions<Token> & Restrictions<Token>) &
  ((
    trigger: Pattern<Token> | Pattern<Token>[] | null,
    output: Output<Token>
  ) => Actions & Conditions<Token> & Restrictions<Token>);

type Actions<R = void> = {
  readonly call: ((task: (executor: Executor) => Promise<unknown>) => R) &
    ((label: string, task: (executor: Executor) => Promise<unknown>) => R);
};

type Restrictions<Token extends string> = {
  readonly only: (options?: RunOptions<Token>) => Actions & Conditions<Token>;
};

type Conditions<Token extends string> = {
  readonly if: (clause: Clause<Token>) => Actions<Recovery>;
};

type Recovery = {
  readonly else: (value: unknown) => void;
};

type ClauseResult = Promise<{ shouldRun: boolean; note?: string }>;
type EvalClause<Token extends string = string> = (
  data?: Partial<Record<Token, unknown>>
) => ClauseResult;

type Clause<Token extends string> =
  | { modified: string | string[] }
  | { expired: string | string[]; ttl: Millis }
  | { missing: string | string[] }
  | EvalClause<Token>;

type Millis = number | `${number}s` | `${number}m` | `${number}h`;

type Pattern<Token extends string> = Token | `?${Token}` | `!${Token}`;
type Output<Token> = { make: Token };

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
  allowSkipping: boolean;
  evalClause?: EvalClause;
  fileClause?: FileClause;
  getResult?: () => unknown;
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
  Token extends string = Source extends string
    ? "start" | "0" | Source
    : "start" | "0" | (keyof Source & string)
>(define: Configurator<Token>): Driver<Token> {
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

      // allows defining tasks after schedule function
      await Promise.resolve();

      const steps: Step[] = [];
      let execution: "full" | "focused" = "full";
      let verboseOverride: boolean | undefined = undefined;
      const when: Matcher<Token> = (
        trigger: Pattern<Token> | Pattern<Token>[] | null,
        output?: Output<Token>
      ) => {
        let focusedOptions: RunOptions<Token> | undefined = undefined;
        let taskClause: Clause<string> | undefined = undefined;
        let taskResult: unknown = undefined;
        const getResult = () => taskResult;
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
            const requirements: { token: string; expect: boolean }[] = [];
            if (!focusedOptions) {
              asList(trigger).forEach((pattern) => {
                if (pattern.startsWith("!") || pattern.startsWith("?")) {
                  const token = pattern.slice(1);
                  input.push(validateToken(token));
                  requirements.push({ token, expect: pattern.startsWith("?") });
                } else {
                  input.push(validateToken(pattern));
                }
              });
            } else if (execution === "focused") {
              input.push(String(steps.length));
            }
            validateToken(output?.make);
            const isQualified = (
              state: Record<string, unknown>,
              allowUndefined?: boolean
            ) =>
              (focusedOptions ? [] : requirements).every(
                (condition) =>
                  (allowUndefined && state[condition.token] === undefined) ||
                  !!state[condition.token] === condition.expect
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
                allowSkipping:
                  !focusedOptions ||
                  (focusedOptions.skip ?? "never") !== "never",
                evalClause: readEvalClause(taskClause),
                fileClause: readFileClause(taskClause),
                getResult,
                data: focusedOptions?.data,
              });
            }
          },
        };

        const skippable = {
          ...actions,
          if: (clause: Clause<string>) => {
            taskClause = clause;
            const recovery: Recovery = {
              else: (value) => (taskResult = value),
            };
            return {
              call: (arg0, arg1?: Parameters<typeof actions.call>[1]) => {
                actions.call(arg0, arg1);
                return recovery;
              },
            } satisfies Actions<Recovery>;
          },
        };

        return {
          ...skippable,
          only: (options?: TaskOptions<Token>) => {
            focusedOptions = options ?? {};
            return skippable;
          },
        };
      };

      const make = (make: Token) => ({ make });
      define(when, make);

      const config = options.data ?? ({} as Record<string, unknown>);
      const runnable = steps.filter((it) => it.isQualified(config, true));
      const inputs = new Set(runnable.flatMap((step) => step.input));
      const done = new Set(
        [...inputs].filter((key) => config && config[key] !== undefined)
      );
      done.add("start").add("0");

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
      const history = new History(".aftershell", reporter.logger);

      if (verbose && runnable.length < steps.length) {
        for (const skipped of steps) {
          if (!runnable.includes(skipped)) {
            const info = color.dim(skipped.title);
            reporter.logger?.log("SKIPPING", info, color.gray("@00"));
          }
        }
      }

      const activateStep = (group: Group) => async (step: Step) => {
        let shouldRun = false;
        let note: string | undefined = undefined;
        if (step.evalClause || step.fileClause) {
          const result: unknown =
            (await step.evalClause?.(step.data)) ??
            (await checkFiles(step.fileClause, history));
          if (!result || typeof result !== "object") {
            const actual = result == null ? result : typeof result;
            note = `(got ${actual}, expected object)`;
          } else if (!("shouldRun" in result)) {
            note = `(missing shouldRun value)`;
          } else {
            shouldRun = Boolean(result.shouldRun);
            note = "note" in result ? String(result.note) : "";
          }
          if (!shouldRun && step.allowSkipping) {
            const info =
              color.dim(step.title) + (note ? " " + color.magenta(note) : "");
            reporter.logger?.log("SKIPPING", info, color.gray("@00"));
            reporter.puffer?.emit(`${reporter.symbols.skip} ${info}\n`);
            if (step.output) {
              group.activate(step.output, step.getResult?.());
            }
            return;
          }
        } else {
          shouldRun = true;
        }
        if (!shouldRun) {
          note = "(skip disabled)";
        }
        reporter.start(group, step, note);
      };

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
          ready.forEach(activateStep(this));
        },
      };

      const cancelTasks = () => {
        if (runtime.status === "active") {
          runtime.status = "cancelled";
          runtime.exitCause = { interrupt: "SIGINT" };
          if (runtime.isInteractive && reporter.logger) {
            reporter.logger.shouldAddNewLine = true;
          }
        } else if (
          runtime.status === "failed" ||
          runtime.status === "cancelled"
        ) {
          process.exit(1);
        }
      };
      process.on("SIGINT", cancelTasks);

      ready.forEach(activateStep(group));

      let success = false;
      try {
        await reporter.result();
        success = true;
      } finally {
        process.off("SIGINT", cancelTasks);
        await history.close({ merge: !success || execution !== "full" });
      }
    },
  };
}

export { execa } from "execa";

type TaskTracker = {
  title: string;
  titleTag?: string;
  step: Step;
  marker: string;
  activities: Activity[];
};

type Activity = {
  title: string;
  status: "active" | "command" | "done" | "fail";
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
    done: color.green("✔"),
    fail: color.red("✖"),
    stop: color.red("◼"),
    skip: color.yellow("∅"),
  };

  start(group: Group, step: Step, note?: string) {
    const wid = color.cyan(this.issueId());
    const tracker: TaskTracker = {
      step,
      title: step.title,
      marker: wid,
      activities: [],
    };
    const stepStart = Date.now();
    const executor = (...args: Parameters<Executor>) => {
      const printer = this.logger;
      const print = function* (line: string) {
        printer?.log("OUTPUT", String(line), wid);
        yield line;
      };
      let run = exec$;
      const first = args[0];
      if (
        typeof first === "object" &&
        !Array.isArray(first) &&
        !("raw" in first)
      ) {
        if (!("stdout" in first)) {
          (first as any).stdout = print as never;
        }
        if (!("stderr" in first)) {
          (first as any).stderr = print as never;
        }
      } else {
        run = exec$({ stdout: print, stderr: print } as any) as never;
      }
      const x = run(...args);
      if ("spawnargs" in x) {
        this.logger?.log("COMMAND", `${x.spawnargs.join(" ")}`, wid);
        const title = x.spawnargs.join(" ");
        const activity: Activity = { title, status: "command" };
        tracker.activities.push(activity);
        const deactivate = () => {
          this.logger?.end(wid);
          activity.status = "done";
        };
        x.then(deactivate).catch(deactivate);
      }
      return x;
    };

    const read: Executor["read"] = (first, ...rest) => {
      if (first === "stdout" || first === "stderr") {
        const reader: ExecWithGlob<Promise<string>> = async (...args) => {
          const result = await exec$(...args);
          return String(first === "stdout" ? result.stdout : result.stderr);
        };
        return reader as any;
      } else {
        const run = exec$({ all: true });
        return run(first, ...(rest as never[])).then((r) => r.all);
      }
    };
    Object.assign(executor, { read });

    const worker: Worker = {
      data: group.data,
      displayTitle: (title: string) =>
        void (tracker.title = color.bold(stripAnsi(title))),
      displayTitleTag: (tag: string) =>
        void (tracker.titleTag = stripAnsi(tag)),
      addActivity: (title: string) => {
        const activity: Activity = {
          title: stripAnsi(title),
          status: "active",
        };
        this.logger?.log("INIT", activity.title, wid);
        tracker.activities.push(activity);
        return (result) => {
          activity.status = result instanceof Error ? "fail" : "done";
          this.logger?.log(
            activity.status === "done" ? "DONE" : "FAIL",
            activity.title,
            wid
          );
        };
      },
      $: executor as unknown as Executor,
    };
    this.running.push(tracker);
    const start = note ? `${step.title} ${color.magenta(note)}` : step.title;
    this.logger?.log("STARTING", start, wid);
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
              this.logger?.log("COMPLETED", `Done in ${duration}`);
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
              this.logger?.log("COMPLETED", color.red(`Cancelled${info}`));
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
        tracker.activities.forEach((activity, index, all) => {
          const connector = color.gray(index === all.length - 1 ? "└╸" : "├╸");
          const status =
            activity.status === "done"
              ? this.symbols.done
              : activity.status === "fail"
              ? this.symbols.fail
              : activity.status === "command"
              ? color.bold(color.cyanBright("$"))
              : color.bold(color.cyanBright("▸"));
          const short = activity.title.slice(0, (stdout.columns ?? 80) - 8);
          line += `  ${color.dim(connector)}${status} ${short}\n`;
        });
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
  | ("INIT" | "DONE" | "FAIL")
  | ("SKIPPING" | "COMPLETED" | "ALERT" | "COMMAND" | "OUTPUT");
class Logger {
  stream = process.stdout;
  tagColors: Partial<Record<LoggerAction, typeof color.dim>> = {
    STARTING: color.yellow,
    FINISHED: color.green,
    SKIPPING: color.gray,
    FAILED: color.red,
    CANCELLED: color.red,
    INIT: color.gray,
    DONE: color.gray,
    FAIL: color.gray,
    ALERT: color.red,
  };
  shouldAddNewLine = false;
  gutterColors = [
    color.bgYellowBright,
    color.bgGreenBright,
    color.bgWhiteBright,
    color.bgCyan,
    color.bgMagenta,
    color.bgGray,
    color.bgBlue,
    color.bgGreen,
    color.bgRed,
  ];
  mainColorCount = 4;
  nextColor = 0;
  gutterUsage = new Map<string, number>();
  gutterCounters = this.gutterColors.map(() => 0);
  messageColors: Partial<Record<LoggerAction, typeof color.dim>> = {
    STARTING: color.gray,
  };
  symbols: Partial<Record<LoggerAction, string>> = {
    COMMAND: "$",
    OUTPUT: "›",
    COMPLETED: "",
    INIT: "→",
    DONE: "→",
    FAIL: "→",
  };
  log(action: LoggerAction, message: string, stepMarker = "") {
    const ts = getFormattedTimestamp();
    const paint = this.tagColors[action] ?? identity;
    const print = this.messageColors[action] ?? identity;
    const symbol = this.symbols[action] ?? "~";
    const gutter =
      action === "OUTPUT" ? this.getColor(stepMarker)(symbol) : symbol;
    const id = stepMarker || symbol ? ` ${stepMarker}${gutter}` : "";
    const correction = this.shouldAddNewLine ? "\n" : "";
    this.shouldAddNewLine = false;
    const prefix = `${correction}${color.dim(`[${ts}]`)}${id}`;
    const annotation =
      action === "OUTPUT" || action === "COMMAND" || action === "COMPLETED"
        ? ""
        : paint(`[${action}] `);
    this.stream.write(`${prefix} ${annotation}${print(message)}\n`);
  }
  end(stepMarker: string) {
    this.gutterUsage.delete(stepMarker);
  }
  getColor(id: string): typeof color.red {
    const preselected = this.gutterUsage.get(id);
    if (preselected !== undefined) {
      return this.gutterColors[preselected];
    }
    if (this.gutterCounters[this.nextColor] === 0) {
      this.gutterCounters[this.nextColor] += 1;
      const result = this.gutterColors[this.nextColor];
      this.gutterUsage.set(id, this.nextColor);
      this.nextColor = (this.nextColor + 1) % this.mainColorCount;
      return result;
    } else {
      const index = indexOfMin(this.gutterCounters);
      this.gutterCounters[index] += 1;
      this.gutterUsage.set(id, index);
      this.nextColor =
        index < this.mainColorCount
          ? (index + 1) % this.mainColorCount
          : this.nextColor;
      return this.gutterColors[index];
    }
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

function withGlob(original: typeof build$, cwd?: string): GlobExecutor {
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

export class History {
  loaded: null | Promise<Record<string, string[]>> = null;
  updated: Record<string, string[] | undefined> = {};

  constructor(public path: string, public logger?: Logger) {}

  load(key: string): Promise<string[] | undefined> {
    return (
      this.loaded ||
      (this.loaded = fs
        .readFile(this.path, "utf8")
        .then((text) => JSON.parse(text))
        .catch((e) => {
          if (!String(e).includes("ENOENT")) {
            const alert = `Failed to read file ${this.path}: ${e}`;
            this.logger?.log("ALERT", alert);
          }
          return {};
        }))
    ).then((data) => (data?.[key] ? toStringArray(data[key]) : undefined));
  }

  save(key: string, value: string[] | undefined) {
    this.updated[key] = value;
  }

  async close({ merge }: { merge: boolean }) {
    if (this.loaded) {
      try {
        const data = merge ? { ...this.loaded, ...this.updated } : this.updated;
        await fs.writeFile(this.path, JSON.stringify(data), "utf8");
      } catch (e) {
        const alert = `Failed to write file ${this.path}: ${e}`;
        this.logger?.log("ALERT", alert);
      }
    }
  }
}

type FileClause =
  | {
      type: "expired";
      patterns: string[];
      ttl: number;
    }
  | {
      type: "missing" | "modified";
      patterns: string[];
      key: string;
    };

function readEvalClause(clause?: Clause<string>): EvalClause | undefined {
  return typeof clause === "function" ? clause : undefined;
}

function readFileClause(clause?: Clause<string>): FileClause | undefined {
  if (!clause || typeof clause === "function") {
    return undefined;
  }
  if ("expired" in clause) {
    const patterns = toStringArray(clause.expired);
    return { type: "expired", patterns, ttl: parseMillis(clause.ttl) };
  }
  if ("missing" in clause) {
    const patterns = toStringArray(clause.missing);
    return {
      type: "missing",
      patterns,
      key: JSON.stringify({ missing: [...patterns].sort() }),
    };
  }
  if ("modified" in clause) {
    const patterns = toStringArray(clause.modified);
    return {
      type: "modified",
      patterns,
      key: JSON.stringify({ modified: [...patterns].sort() }),
    };
  }
  const residue: never = clause;
  return void residue;
}

function toStringArray(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.map((item) => String(item));
  }
  return [String(value)];
}

type Recording = {
  load: (key: string) => Promise<string[] | undefined>;
  save: (key: string, value: string[] | undefined) => void;
};

async function checkFiles(
  clause: FileClause | undefined,
  recording: Recording
): ClauseResult {
  if (!clause) {
    return { shouldRun: true };
  }

  if (clause.type === "modified") {
    const found = (await fg.async(clause.patterns)).sort();

    if (found.length === 0) {
      recording.save(clause.key, undefined);
      return {
        shouldRun: true,
        note: `(Nothing found for ${clause.patterns.join(";")})`,
      };
    }

    const next = new Map<string, string>();
    const prev = new Map<string, string>();
    await Promise.all(found.map(async (f) => next.set(f, await sha1(f))));
    const saved = await recording.load(clause.key);
    saved?.forEach((slug) => {
      const [file, hash] = slug.split("///");
      prev.set(file, hash);
    });
    const data = [...next.entries()].map(([k, v]) => `${k}///${v}`);
    recording.save(clause.key, data);

    const list = compareContents([...prev.keys()], [...next.keys()]);

    if (!list.same) {
      if (list.new.length > 0) {
        return {
          shouldRun: true,
          note: `(Found ${describeListContents(list.new)})`,
        };
      }
      if (list.missing.length > 0) {
        return {
          shouldRun: true,
          note: `(Missing ${describeListContents(list.missing)})`,
        };
      }
    }

    const modified = found.filter((f) => next.get(f) !== prev.get(f));
    if (modified.length > 0) {
      return {
        shouldRun: true,
        note: `(Modified ${describeListContents(modified)})`,
      };
    }

    return {
      shouldRun: false,
      note: `(Matched ${pluralize(found.length, "file")})`,
    };
  }

  if (clause.type === "expired") {
    const found = (await fg.async(clause.patterns)).sort();

    if (found.length === 0) {
      return {
        shouldRun: true,
        note: `(Nothing found for ${clause.patterns.join(";")})`,
      };
    }

    let oldest = found[0];
    let oldestAge: null | number = null;
    for (let i = 0, l = found.length; i < l; i++) {
      const file = found[i];
      const { mtime } = await fs.stat(file);
      const age = Date.now() - mtime.getTime();
      if (oldestAge === null || age > oldestAge) {
        oldest = file;
        oldestAge = age;
      }
    }

    const diff = (oldestAge ?? 0) - clause.ttl;
    if (diff >= 0) {
      return {
        shouldRun: true,
        note: `(${oldest} is expired for ${formatDuration(diff)})`,
      };
    }

    return {
      shouldRun: false,
      note: `(${oldest} will expire in ${formatDuration(-diff)})`,
    };
  }

  if (clause.type === "missing") {
    const groups = await Promise.all(clause.patterns.map((it) => fg.async(it)));
    const found = groups.map((group) => group.sort()).flat();

    for (let i = 0, l = groups.length; i < l; i++) {
      if (groups[i].length === 0) {
        recording.save(clause.key, undefined);
        return {
          shouldRun: true,
          note: `(Nothing found for ${clause.patterns[i]})`,
        };
      }
    }

    const previouslyFound = await recording.load(clause.key);
    recording.save(clause.key, found);

    if (previouslyFound) {
      const list = compareContents(previouslyFound, found);
      if (list.missing.length > 0) {
        return {
          shouldRun: true,
          note: `(Missing ${describeListContents(list.missing)})`,
        };
      }
    }

    const note = `(Found ${found[0]}${
      found.length === 1 ? "" : ` +${found.length - 1} more`
    })`;

    return { shouldRun: false, note };
  }

  return { shouldRun: true, note: "(invalid config)" };
}

function compareContents(
  a1: string[],
  a2: string[]
): { same: boolean; new: string[]; missing: string[] } {
  const s1 = new Set(a1);
  const missed = new Set(s1);
  const result = { same: true, new: [] as string[], missing: [] as string[] };
  for (let i = 0, l = a2.length; i < l; i++) {
    const value = a2[i];
    if (!s1.has(value)) {
      result.same = false;
      result.new.push(value);
    } else {
      missed.delete(value);
    }
  }
  if (missed.size > 0) {
    result.same = false;
    result.missing = [...missed];
  }
  return result;
}

function describeListContents(items: string[]): string {
  if (items.length === 0) {
    return "";
  } else if (items.length === 1) {
    return items[0];
  } else {
    return `${items[0]} +${items.length - 1} more`;
  }
}

export async function sha1(path: string): Promise<string> {
  return new Promise((resolve, reject) => {
    import("node:fs").then((fs) => {
      const hash = crypto.createHash("sha1");
      const stream = fs.createReadStream(path);
      stream.on("error", reject);
      stream.on("data", (chunk) => hash.update(chunk));
      stream.on("end", () => resolve(hash.digest("base64")));
    });
  });
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
  let hours = Math.floor(millis / 3600000);
  let minutes = Math.floor(millis / 60000);
  let seconds = ((millis - minutes * 60000) / 1000).toFixed(0);
  if (seconds === "60") {
    minutes += 1;
    seconds = "00";
  }
  return hours < 2
    ? `${minutes.toFixed(0)}m${seconds.padStart(2, "0")}s`
    : `${hours.toFixed(0)}h${(Math.floor((millis % 3600000) / 6000) / 10)
        .toFixed(1)
        .padStart(4, "0")}m`;
}

function parseMillis(value: number | string): number {
  if (typeof value === "number") {
    return value;
  }
  const match = value.match(/^(\d+)(h|m|s)$/);
  if (!match) {
    throw new TypeError(`Cannot parse time from "${value}"`);
  }
  const amount = Number(match[1]);
  if (match[2] === "s") {
    return amount * 1000;
  } else if (match[2] === "m") {
    return amount * 60 * 1000;
  }
  return amount * 60 * 60 * 1000;
}

function indexOfMin(data: number[]): number {
  let min = data[0];
  let minIndex = 0;
  for (let i = 1; i < data.length; i++) {
    if (min === undefined || data[i] < min) {
      min = data[i];
      minIndex = i;
    }
  }
  return minIndex;
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
function validateToken<T>(value: T): T {
  if (typeof value === "string") {
    const res = ReservedSymbol.exec(value);
    if (res) {
      throw new Error(`Reserved symbol ${res} found in ${value}`);
    }
  }
  return value;
}

function pluralize(count: number, text1: string, textMore?: string): string {
  return count + " " + (count === 1 ? text1 : textMore ?? text1 + "s");
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

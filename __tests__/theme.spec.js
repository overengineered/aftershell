function dissectAnsi(input) {
  const output = [];
  const length = input.length;
  let activeStyle = [];
  let index = 0;
  let buffer = "";
  while (index < length) {
    const start = index;
    index = input.indexOf("\x1b", start);
    // console.log(`at ${start} [${input.slice(start, start + 3)}] -> ${index}`);
    if (index < 0) {
      buffer += input.slice(start);
      break;
    }
    buffer += input.slice(start, index);
    let prev = input.codePointAt(index);
    index += 1;
    for (let offset = index; offset < length; offset += 1) {
      const cp = input.codePointAt(offset);
      const isValid =
        cp > 128
          ? false
          : offset === index
          ? cp === "[".codePointAt(0)
          : prev === "[".codePointAt(0) || prev === ";".codePointAt(0)
          ? cp >= "0".codePointAt(0) && cp <= "9".codePointAt(0)
          : cp === ";".codePointAt(0) ||
            cp === "m".codePointAt(0) ||
            (cp >= "0".codePointAt(0) && cp <= "9".codePointAt(0));
      prev = cp;
      if (!isValid) {
        buffer += input.slice(index - 1, offset + 1);
        index = offset + 1;
        break;
      } else if (cp === "m".codePointAt(0)) {
        const newStyle = input.slice(index + 1, offset).split(";");
        if (isSensible(newStyle)) {
          if (buffer.length > 0) {
            output.push({ text: buffer, ansi: activeStyle });
            buffer = "";
          }
          activeStyle = newStyle;
        } else {
          buffer += input.slice(index - 1, offset + 1);
        }
        index = offset + 1;
        break;
      }
    }
  }

  // const ansi = styleArray(brush);
  if (buffer.length > 0 || activeStyle.length > 0) {
    output.push({ ansi: activeStyle, text: buffer });
  }

  return output;
}

function isSensible(elements) {
  const used = new Set();
  for (let i = 0, l = elements.length; i < l; i += 1) {
    const key = elements[i];
    if (key === "0") {
      return elements.length === 1;
    } else if (key === "22") {
      if (used.has("b") || used.has("f")) {
        return false;
      }
      used.add("b").add("f");
    } else {
      const a = Attributes[key];
      // console.log(key, a, [...used]);
      if (a === undefined || used.has(a)) {
        return false;
      }
      used.add(a);
    }
  }
  return true;
}

// let text = "↴↳⏎⇒";

// function styleArray(brush) {
//   return [...new Set(Object.values(brush))];
// }

// function updateBrush(brush, config) {
//   const next = {};
//   for (const value of config) {
//     if (value === "0") {
//       for (const key of Object.keys(brush)) {
//         next[key] = "0";
//       }
//       continue;
//     }
//     const attribute = Attributes[value];
//     if (attribute == null) {
//       return false;
//     }
//     for (const key of attribute) {
//       if (key in next) {
//         return false;
//       }
//       next[key] = value;
//     }
//   }
//   Object.assign(brush, next);
//   return true;
// }

const Attributes = {
  1: "b",
  2: "f",
  3: "i",
  4: "u",
  9: "s",
  22: "bf",
  23: "i",
  24: "u",
  29: "s",
  30: "c",
  31: "c",
  32: "c",
  33: "c",
  34: "c",
  35: "c",
  36: "c",
  37: "c",
  39: "c",
  40: "g",
  41: "g",
  42: "g",
  43: "g",
  44: "g",
  45: "g",
  46: "g",
  47: "g",
  49: "g",
  53: "o",
  55: "o",
  90: "c",
  91: "c",
  92: "c",
  93: "c",
  94: "c",
  95: "c",
  96: "c",
  97: "c",
  100: "g",
  101: "g",
  102: "g",
  103: "g",
  104: "g",
  105: "g",
  106: "g",
  107: "g",
};

function validatable(text) {
  let r = "";
  for (const c of text) {
    const cp = c.codePointAt(0);
    if (cp === 0x1b) {
      r += `\\x1b`;
    } else if (cp < 32) {
      r += "(\\x" + c.codePointAt(0).toString("16") + ")";
    } else if (c === "\\") {
      r += "⍉";
    } else if (c === "⍉") {
      r += "\\⍉";
    } else {
      r += c;
    }
  }
  return r;
}

// const uut = (t, v, l) => validatable(decorate(t, v, l));

describe("styleString", () => {
  it("should find colored span that encompasses all string", () => {
    expect(dissectAnsi("\x1b[31mRED\x1b[39m")).toEqual([
      { ansi: ["31"], text: "RED" },
      { ansi: ["39"], text: "" },
    ]);
  });

  // it handle unterminated "valid" theming

  it("should find two colored spans", () => {
    expect(dissectAnsi("\x1b[31mRED\x1b[34mBLUE\x1b[39m")).toEqual([
      { ansi: ["31"], text: "RED" },
      { ansi: ["34"], text: "BLUE" },
      { ansi: ["39"], text: "" },
    ]);
  });

  it("should find colored span at the start of input followed by unstyled text", () => {
    expect(dissectAnsi("\x1b[31mRED\x1b[39mPLAIN")).toEqual([
      { ansi: ["31"], text: "RED" },
      { ansi: ["39"], text: "PLAIN" },
    ]);
  });

  it("should find span terminated by reset", () => {
    expect(dissectAnsi("\x1b[31mRED\x1b[0mPLAIN")).toEqual([
      { ansi: ["31"], text: "RED" },
      { ansi: ["0"], text: "PLAIN" },
    ]);
  });

  it("should find span that is dim and bold", () => {
    expect(dissectAnsi("\x1b[1;2mfaint-bold\x1b[22m")).toEqual([
      { ansi: ["1", "2"], text: "faint-bold" },
      { ansi: ["22"], text: "" },
    ]);
  });

  it("should find multiple spans", () => {
    expect(dissectAnsi("\x1b[1mBOLD\x1b[0mPLAIN\x1b[4mUNDER\x1b[24m")).toEqual([
      { ansi: ["1"], text: "BOLD" },
      { ansi: ["0"], text: "PLAIN" },
      { ansi: ["4"], text: "UNDER" },
      { ansi: ["24"], text: "" },
    ]);
  });

  it("should find overlapping spans", () => {
    expect(dissectAnsi("\x1b[31m\x1b[32mW\x1b[33mEIRD\x1b[39m")).toEqual([
      { ansi: ["32"], text: "W" },
      { ansi: ["33"], text: "EIRD" },
      { ansi: ["39"], text: "" },
    ]);
  });

  it("should find overlapping spans", () => {
    expect(dissectAnsi("\x1b[1m\x1b[32mW\x1b[33mEIRD\x1b[22;39m")).toEqual([
      { ansi: ["1", "32"], text: "W" },
      { ansi: ["33"], text: "EIRD" },
      { ansi: ["39"], text: "" },
    ]);
  });

  it("should keep unsupported style settings intact", () => {
    expect(
      dissectAnsi(`HACK\x1b[13m`).map((it) => {
        return { ...it, text: validatable(it.text) };
      })
    ).toEqual([{ ansi: [], text: "HACK\\x1b[13m" }]);
  });

  it("should keep unsupported style settings intact", () => {
    expect(
      dissectAnsi(`HACK\x1b[13m`).map((it) => {
        return { ...it, text: validatable(it.text) };
      })
    ).toEqual([{ ansi: [], text: "HACK\\x1b[13m" }]);
  });

  it("should keep invalid style settings intact", () => {
    expect(
      dissectAnsi(`abc\x1b[10;whatever!mxyz`).map((it) => {
        return { ...it, text: validatable(it.text) };
      })
    ).toEqual([{ ansi: [], text: "abc\\x1b[10;whatever!mxyz" }]);
  });

  it("should keep ambiguous style settings intact", () => {
    expect(
      dissectAnsi(`abc\x1b[33;34;35;m?which-color?`).map((it) => {
        return { ...it, text: validatable(it.text) };
      })
    ).toEqual([{ ansi: [], text: "abc\\x1b[33;34;35;m?which-color?" }]);
  });

  // it("should allow restricting number of symbols", () => {
  //   expect(
  //     uut(["1"], `BOLD\x1b[0mPLAIN\x1b[4mUNDER`, { maxLength: 3 })
  //   ).toEqual("\\x1b[1mBOL\\x1b[24m");
  // });

  // it("should deal with combining diacritics when output is restricted", () => {
  //   expect(uut(["33"], `ə̀̈ə̀̈ə̀̈`, { maxLength: 2 })).toEqual(
  //     "\\x1b[33mə̀̈ə̀̈\\x1b[39m"
  //   );
  // });

  // it("should deal with unsupported styling when output is restricted", () => {
  //   expect(uut(["9"], `abc\x1b[10;10;10m`, { maxLength: 12 })).toEqual(
  //     "\\x1b[9mabc⍉x1b[10;1\\x1b[29m"
  //   );
  // });

  // it("should deal with invalid styling when output is restricted", () => {
  //   expect(uut(["9"], `abc\x1b[10;a0;10m`, { maxLength: 12 })).toEqual(
  //     "\\x1b[9mabc⍉x1b[10;a\\x1b[29m"
  //   );
  // });

  // it("should replace newlines when output is restricted", () => {
  //   expect(uut(["53"], `1\n2\n\n3\n\n\n`, { maxLength: 12 })).toEqual(
  //     "\\x1b[53mabc⍉x1b[10;a\\x1b[55m"
  //   );
  // });

  // it("should deal with styled emojis when output is restricted", () => {
  //   expect(uut(["42"], `🧘🏾‍♀️🧘🏻‍♂️🧘🏼‍♀️`, { maxLength: 2 })).toEqual(
  //     "\\x1b[42m🧘🏾\\x1b[49m" // not ideal, but realistic (VS Code terminal prints 2 symbols)
  //   );
  // });
});

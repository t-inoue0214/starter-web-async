#!/usr/bin/env python3
"""chapters/*.md のコードブロックを src/ の完成コードで差し替える執筆者用ツール。

章の中に次のように書いておくと、コメント直後のコードブロックの中身を置き換える。

    <!-- code: src/main/java/com/example/async/p2p/HelloProducer.java -->
    ```java
    （ファイル全体に置き換わる）
    ```

    <!-- code: src/main/java/com/example/async/p2p/HelloConsumer.java#handleDelivery -->
    ```java
    （handleDelivery メソッドだけに置き換わる。直前の Javadoc・コメント・注釈も含む）
    ```

`#` の後ろはメソッド名（コンストラクタ名も可）。`#main,placeOrder` のように
カンマ区切りで複数指定すると、「// （中略）」を挟んで並べる。
教材の文章と、実際に動作確認したコードがずれないようにするためのもの。
使い方: python3 tools/sync-chapter-code.py
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
PATTERN = re.compile(
    r"(<!-- code: (?P<path>[^ #]+)(?:#(?P<members>[\w,]+))? -->\n```(?P<lang>\w+)\n)(?P<body>.*?)(```)", re.S)
COMMENT_OR_ANNOTATION = re.compile(r"^\s*(//|/\*\*|\*|\*/|@\w+)")


def find_declaration(lines: list[str], name: str) -> int:
    """メソッド（またはコンストラクタ）の宣言行の位置を返す。呼び出し行は除く。"""
    decl = re.compile(r"^\s*(?:(?:public|private|protected|static|final|synchronized|abstract)\s+)*"
                      r"(?:[\w<>\[\],.? ]+\s+)?" + re.escape(name) + r"\s*\(")
    for i, line in enumerate(lines):
        if decl.match(line) and not line.rstrip().endswith(";") and "=" not in line.split(name)[0] \
                and "return" not in line.split(name)[0] and "new " not in line.split(name)[0]:
            return i
    raise ValueError(f"メソッド {name} が見つかりません")


def block_end(lines: list[str], start: int) -> int:
    """start 行から始まる波かっこのブロックが閉じる行の位置を返す（文字列とコメントの中は数えない）。"""
    depth = 0
    seen_open = False
    for i in range(start, len(lines)):
        line = lines[i]
        j = 0
        in_str = None
        while j < len(line):
            c = line[j]
            if in_str:
                if c == "\\":
                    j += 2
                    continue
                if c == in_str:
                    in_str = None
            elif c in "\"'":
                in_str = c
            elif line.startswith("//", j):
                break
            elif c == "{":
                depth += 1
                seen_open = True
            elif c == "}":
                depth -= 1
                if seen_open and depth == 0:
                    return i
            j += 1
    raise ValueError("ブロックの終わりが見つかりません")


def extract(source: str, members: str) -> str:
    lines = source.splitlines()
    parts = []
    for name in members.split(","):
        decl = find_declaration(lines, name)
        begin = decl
        while begin > 0 and COMMENT_OR_ANNOTATION.match(lines[begin - 1]):
            begin -= 1
        end = block_end(lines, decl)
        chunk = lines[begin:end + 1]
        indent = min(len(l) - len(l.lstrip()) for l in chunk if l.strip())
        parts.append("\n".join(l[indent:] for l in chunk))
    return "\n\n// （中略）\n\n".join(parts) + "\n"


def main() -> int:
    errors = []
    for md in sorted((ROOT / "chapters").glob("*.md")):
        text = md.read_text(encoding="utf-8")

        def replace(m: re.Match) -> str:
            src = ROOT / m.group("path")
            if not src.exists():
                errors.append(f"{md.name}: 見つからないファイル {m.group('path')}")
                return m.group(0)
            body = src.read_text(encoding="utf-8")
            if m.group("members"):
                try:
                    body = extract(body, m.group("members"))
                except ValueError as e:
                    errors.append(f"{md.name}: {m.group('path')} {e}")
                    return m.group(0)
            if not body.endswith("\n"):
                body += "\n"
            return m.group(1) + body + m.group(6)

        new = PATTERN.sub(replace, text)
        if new != text:
            md.write_text(new, encoding="utf-8")
            print(f"updated {md.relative_to(ROOT)}")
    for item in errors:
        print(item, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())

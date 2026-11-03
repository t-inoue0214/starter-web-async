#!/usr/bin/env python3
"""chapters/*.md のコードブロックを answers/ の完成コードで差し替える執筆者用ツール。

章の中に
    <!-- code: answers/src/main/java/com/example/async/p2p/HelloProducer.java -->
    ```java
    （ここが丸ごと置き換わる）
    ```
と書いておくと、コメント直後のコードブロックの中身を指定ファイルの内容で置き換える。
教材の文章と、実際に動作確認したコードがずれないようにするためのもの。
使い方: python3 tools/sync-chapter-code.py
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
PATTERN = re.compile(r"(<!-- code: (?P<path>[^ ]+) -->\n```(?P<lang>\w+)\n)(?P<body>.*?)(```)", re.S)


def main() -> int:
    missing = []
    for md in sorted((ROOT / "chapters").glob("*.md")):
        text = md.read_text(encoding="utf-8")

        def replace(m: re.Match) -> str:
            src = ROOT / m.group("path")
            if not src.exists():
                missing.append(f"{md.name}: {m.group('path')}")
                return m.group(0)
            body = src.read_text(encoding="utf-8")
            if not body.endswith("\n"):
                body += "\n"
            return m.group(1) + body + m.group(5)

        new = PATTERN.sub(replace, text)
        if new != text:
            md.write_text(new, encoding="utf-8")
            print(f"updated {md.relative_to(ROOT)}")
    for item in missing:
        print(f"見つからないファイル: {item}", file=sys.stderr)
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())

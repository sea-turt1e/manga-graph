"""Runtime environment check.

eMudhra intermediate CA key compromise (https://emudhra.we-have-a-problem.com/)
への対応として、修正済みの Python / certifi 以外で実行されている場合に警告を出す。
標準ライブラリのみに依存するので、どのエントリポイントからでも import できる。

単体で確認する場合: python -m config.python_version_check
"""

import sys
import warnings

# 修正済み Python バージョン（README の「対応 Python バージョン」と揃えること）
SUPPORTED_PYTHON_VERSIONS = (
    (3, 15, 1),
    (3, 14, 9),
    (3, 13, 17),
)

# 修正済み certifi の最小バージョン
MIN_CERTIFI_VERSION = (2026, 10, 19)


class UnsupportedEnvironmentWarning(UserWarning):
    """Raised (as a warning) when the runtime is not one of the patched versions."""


def _format_version(version):
    return ".".join(str(part) for part in version)


def _parse_version(text):
    parts = []
    for part in text.split("."):
        digits = "".join(ch for ch in part if ch.isdigit())
        if not digits:
            break
        parts.append(int(digits))
    return tuple(parts)


def check_environment():
    """Return a list of problems found in the current runtime (empty if OK)."""
    problems = []

    current = tuple(sys.version_info[:3])
    if current not in SUPPORTED_PYTHON_VERSIONS:
        supported = ", ".join(_format_version(v) for v in SUPPORTED_PYTHON_VERSIONS)
        problems.append(
            f"Python {_format_version(current)} は動作確認対象外です。"
            f"eMudhra CA 問題の修正済みバージョン（{supported}）を使用してください。"
        )

    try:
        import certifi
    except ImportError:
        certifi = None

    if certifi is not None:
        certifi_version = _parse_version(getattr(certifi, "__version__", "0"))
        if certifi_version < MIN_CERTIFI_VERSION:
            problems.append(
                f"certifi {certifi.__version__} は古い CA バンドルを含んでいます。"
                f"certifi>={_format_version(MIN_CERTIFI_VERSION)} に更新してください"
                "（pip install -U -r requirements.txt）。"
            )

    return problems


def warn_if_unsupported_environment():
    """Emit an UnsupportedEnvironmentWarning for each problem found."""
    problems = check_environment()
    for problem in problems:
        warnings.warn(problem, UnsupportedEnvironmentWarning, stacklevel=2)
    return problems


if __name__ == "__main__":
    found = check_environment()
    if found:
        for message in found:
            print(f"WARNING: {message}", file=sys.stderr)
        sys.exit(1)
    print(f"OK: Python {_format_version(sys.version_info[:3])} は修正済みバージョンです。")

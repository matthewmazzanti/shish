fmt:
    ruff format .
    ruff check --fix .

check: lint typecheck test

lint:
    ruff format --check .
    ruff check .

typecheck:
    pyright

test *args:
    pytest {{args}}

build:
    uv build

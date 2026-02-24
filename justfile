test *args:
    pytest {{args}}

check:
    ruff check .
    pyright

fmt:
    ruff format .
    ruff check --fix .

lint:
    ruff check .

typecheck:
    pyright

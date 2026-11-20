"""Explicit additive schema for dormant resumable institutional preparation."""


def main():
    from app.db import engine
    from app.services.direct_13f_preparation import ensure_prepared_13f_schema
    ensure_prepared_13f_schema(engine)
    print('Prepared 13F schema verified; no preparation or publication enabled')


if __name__ == '__main__':
    main()

from sqlalchemy.inspection import inspect


def serialize_model(obj):
    """Converts SQLAlchemy model instance into a JSON-compliant dictionary."""
    return {c.key: getattr(obj, c.key) for c in inspect(obj).mapper.column_attrs}

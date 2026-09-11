from contextvars import ContextVar

# Context variable scoped to the current execution task/request
tenant_context: ContextVar[str] = ContextVar("tenant_context", default="default")
from typing import List, Dict, Any, Optional

def transpile(
    sql: str,
    read: str,
    write: str,
    schema: Optional[Dict[str, Any]] = None,
    **kwargs: Any
) -> List[str]: ...

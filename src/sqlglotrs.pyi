from typing import List, Dict, Any, Optional

def transpile(
    sql: str,
    read: str,
    write: str,
    error_level: Optional[str] = None,
    identity: bool = False,
    **opts,
) -> List[str]: ...

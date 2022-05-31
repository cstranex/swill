# Swill Server - ASGI RPC Application Framework

This is the server component. Use this to server your application and requests

### Example
```python

from typing import List
import swill

app = swill.Swill(__name__)

@app.handle()
def add(request: swill.Request[List[int]]) -> int:
    """Add the numbers in the list"""
    return sum(request.data)
```


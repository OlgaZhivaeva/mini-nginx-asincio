async def app(scope, receive, send):
    assert scope["type"] == "http"

    body = b""
    more_body = True
    while more_body:
        message = await receive()
        body += message.get("body", b"")
        more_body = message.get("more_body", False)

    response_body = b"Echo: " + body if body else b"Hello from Upstream!"

    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [
                [b"content-type", b"text/plain"],
                [b"content-length", str(len(response_body)).encode("ascii")],
            ],
        }
    )
    await send(
        {
            "type": "http.response.body",
            "body": response_body,
        }
    )

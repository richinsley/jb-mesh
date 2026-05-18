"""Canonical persistent/msgpack jb-mesh service starter."""

from jb_service import method, run
from jb_service.msgpack_service import MessagePackService


class MyService(MessagePackService):
    name = "my-service"
    version = "0.1.0"

    def setup(self):
        self.log.info("MyService starting (msgpack)")
        self.state = {"ready": True}

    @method
    def ping(self, message: str = "smoke") -> dict:
        return {"ok": True, "echo": message}

    @method
    def health(self) -> dict:
        return {"status": "ok", "ready": self.state.get("ready", False)}


if __name__ == "__main__":
    run(MyService)

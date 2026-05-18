"""Canonical baseline jb-mesh service starter."""

from jb_service import Service, method, run


class MyService(Service):
    name = "my-service"
    version = "0.1.0"

    def setup(self):
        self.log.info("MyService starting")

    @method
    def ping(self, message: str = "smoke") -> dict:
        return {"ok": True, "echo": message}

    @method
    def health(self) -> dict:
        return {"status": "ok"}


if __name__ == "__main__":
    run(MyService)

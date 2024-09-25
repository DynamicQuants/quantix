from core.broker import hello


class TestBroker:
    def test_hello(self):
        assert hello() == "Hello from Broker"

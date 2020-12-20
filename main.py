import asyncio
from asyncio import get_running_loop

from shakespyeare import BasicActor, Stage


async def main():
    stage = Stage()
    simple_actor = PrintsMessages("+1", name="one", forward="two")
    simple_actor_2 = PrintsMessages("+2", name="two", forward="three")
    simple_actor_3 = PrintsMessages("+3", name="three", forward="four")
    simple_actor_4 = PrintsMessages("+4", name="four", forward="one")
    stage.add_actors(simple_actor, simple_actor_2)
    stage.add_actors(simple_actor_3, simple_actor_4)
    stage.send_message("one", "hello")
    while get_running_loop().is_running():
        await asyncio.sleep(10)


class PrintsMessages(BasicActor):
    def __init__(self, msg_content, name=None, forward=None):
        self.msg_content = msg_content
        self.forward = forward
        super().__init__(name)

    async def handle_started(self):
        print(f"worker {self.name} started")
        return {}

    async def handle_message(self, message, sent_from, state):
        print(message)
        if len(message) > 100:
            message = "TRUNCATED"
        if self.forward:
            self.send_message(self.forward, message + " & " + self.msg_content)
        return state


if __name__ == '__main__':
    asyncio.run(main())



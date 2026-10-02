import asyncio

TEST_LOOP = asyncio.new_event_loop()


def assert_only_current_task_is_alive() -> None:
    assert asyncio.all_tasks() == {asyncio.current_task()}

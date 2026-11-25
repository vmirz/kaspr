from kaspr.faust.utils.queues import EventQueue

from tests.faust.helpers import new_event


def test_clear(*, app):
    queue = EventQueue(flow_control=app.flow_control, clear_on_resume=True)
    events = [new_event(app, 'key', 'value', offset=n) for n in range(3)]
    for event in events:
        event.message.incref(2)  # this queue and another one
        queue.put_nowait(event)
    queue.put_nowait('not an event')
    queue.get_nowait()

    app.flow_control.clear()
    assert queue.empty()
    assert [e.message.dropped for e in events] == [0, 1, 1]
    assert all(e.message.pending for e in events)

    for event in events:
        event.ack()  # the stream of the other queue
    assert [e.message.pending for e in events] == [True, False, False]
    assert not any(e.message.acked for e in events)


def test_app_queues(*, app):
    assert isinstance(app.FlowControlQueue(), EventQueue)
    assert isinstance(app.topic('foo').queue, EventQueue)

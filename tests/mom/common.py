from lazarus.mom.message import Message


def new_msg(name: str, mtype: str, data: str):
    msg = Message()
    msg["from"] = name
    msg["type"] = mtype
    msg["data"] = data
    return msg


def all_finished(finish_table):
    for finished in finish_table.values():
        if not finished:
            return False
    return True

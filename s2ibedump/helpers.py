import json


def toJson(data):
    return json.dumps(data, indent=4, sort_keys=False, default=lambda o: o.__dict__)


def getPlayerSlot(players, slot_id=None, player_id=None, user_id=None):
    for item in players:
        if slot_id is not None and slot_id == item.slot_id:
            return item
        elif player_id is not None and player_id == item.player_id:
            return item
        elif user_id is not None and user_id == item.user_id:
            return item
    return None


def unitTag(unitTagIndex, unitTagRecycle):
    return (unitTagIndex << 18) + unitTagRecycle


def unitTagIndex(unitTag):
    return (unitTag >> 18) & 0x00003fff


def unitTagRecycle(unitTag):
    return (unitTag) & 0x0003ffff

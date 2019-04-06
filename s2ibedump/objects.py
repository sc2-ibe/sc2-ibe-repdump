PLAYER_TYPE_MAP = [
    'FREE',
    'NONE',
    'USER',
    'COMPUTER',
    'NEUTRAL',
    'HOSTILE',
]

GAME_SPEED_MAP = [
    'SLOWER',   # 0
    'SLOW',     # 1
    'NORMAL',   # 2
    'FAST',     # 3
    'FASTER',   # 4
]


def DictAccess(kls):
    kls.__getitem__ = lambda self, attr: getattr(self, attr)
    kls.__setitem__ = lambda self, attr, value: setattr(self, attr, value)
    return kls


@DictAccess
class PlayerSlot(object):
    def __init__(self):
        self.slot_id = None
        self.player_id = None
        self.user_id = None
        self.name = None
        self.clan = None
        self.type = None
        self.handle = None
        self.toon = None
        self.color = None
        self.color_name = None
        self.apm = None

    @classmethod
    def fromParticipant(cls, p):
        result = PlayerSlot()
        result.slot_id = p.sid
        result.player_id = p.pid
        if p.is_human:
            result.user_id = p.uid
            result.clan = p.clan_tag
        else:
            pass
        result.name = p.name
        result.type = PLAYER_TYPE_MAP[p.slot_data['control']]
        result.is_human = p.is_human
        if p.is_human:
            result.handle = p.toon_handle
        result.toon = {
            'region': p.detail_data['bnet']['region'],
            'realm': p.detail_data['bnet']['subregion'],
            'id': p.detail_data['bnet']['uid'],
        }
        result.color = {
            'r': p.color.r,
            'g': p.color.g,
            'b': p.color.b,
            'a': p.color.a,
        }
        result.color_name = p.color.name
        return result

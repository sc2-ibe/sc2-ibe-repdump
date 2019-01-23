#!/usr/bin/env python2
from __future__ import print_function
import sys
import json
from collections import OrderedDict
import mpyq
from s2protocol import versions


class DReader(object):
    def __init__(self, buff):
        self.buff = buff
        self.offset = 0

    def read_bool(self):
        value = True if self.buff[self.offset] else False
        self.offset += 1
        return value

    def read_uint8(self):
        value = self.buff[self.offset]
        self.offset += 1
        return value

    def read_int8(self):
        value = self.read_uint8()
        if value & 0x80:
            value = ~(value) & 0x7F
            value = -value - 1
        return value

    def read_uint16(self):
        value = (
            (self.buff[self.offset] << 8) +
            (self.buff[self.offset + 1])
        )
        self.offset += 2
        return value

    def read_int16(self):
        value = self.read_uint16()
        if value & 0x8000:
            value = ~(value) & 0x7FFF
            value = -value - 1
        return value

    def read_uint32(self):
        value = (
            (self.buff[self.offset] << 24) +
            (self.buff[self.offset + 1] << 16) +
            (self.buff[self.offset + 2] << 8) +
            (self.buff[self.offset + 3])
        )
        self.offset += 4
        return value

    def read_int32(self):
        value = self.read_uint32()
        if value & 0x80000000:
            value = ~(value) & 0x7FFFFFFF
            value = -value - 1
        return value

    def read_fixed32(self):
        value = self.read_uint32()
        if value & 0x80000000:
            value &= 0x7FFFFFFF
            value /= -4096.0
        else:
            value /= 4096.0
        return value


def fetch_payloads_from_tracker(tevents):
    curr_payload = []
    curr_utag_index = None
    for x in tevents:
        if curr_utag_index != None:
            if x['_eventid'] == 2 and x['m_unitTagIndex'] == curr_utag_index:
                yield curr_payload
                curr_utag_index = None
            elif x['_eventid'] in [1, 2]:
                curr_payload.append(x['m_x'])
                curr_payload.append(x['m_y'])
        else:
            if x['_eventid'] == 1 and x['m_unitTypeName'] == '__':
                curr_payload = []
                curr_utag_index = x['m_unitTagIndex']


MAX_PLAYERS = 10
ABIL_MAX = 8
CHALLENGE_MAX = 30
CHALLENGE_POWERUP_MAX = 8
CHALLENGE_BUTTON_MAX = 8

ABIL_MAP = [
    "BOOST",
    "CREEP",
    "THROW_ESSENCE",
    "ART",
    "SHADE_CREATE",
    "SHADE_USE",
    "THROW_ESSENCE_REVIVE",
    "ART_REVIVE"
]

def decode_game_result(payload):
    rd = DReader(payload)
    gmr = OrderedDict()

    gmr['schema_version'] = rd.read_uint16()
    gmr['game_version'] = rd.read_uint16()
    gmr['game_code'] = rd.read_uint16()
    gmr['game_speed'] = True if gmr['game_code'] & (1 << 1) else False
    gmr['game_diff'] = True if gmr['game_code'] & (1 << 0) else False
    gmr['escape_time'] = round(rd.read_fixed32(), 2)
    gmr['escaped'] = gmr['escape_time'] > 0.0
    rd.read_uint8()
    gmr['challenges_completed'] = 0
    gmr['challenges_total'] = rd.read_uint8()

    gmr['players'] = OrderedDict()
    for i in range(1, MAX_PLAYERS + 1):
        exists = rd.read_bool()
        if not exists:
            continue
        gmr['players'][i] = OrderedDict()
        gmr['players'][i]['left'] = rd.read_bool()
        gmr['players'][i]['level'] = rd.read_uint8()
        gmr['players'][i]['deaths'] = rd.read_uint16()
        gmr['players'][i]['revives'] = rd.read_uint16()

        gmr['players'][i]['abilities_used'] = OrderedDict()
        for l in range(0, ABIL_MAX):
            gmr['players'][i]['abilities_used'][ABIL_MAP[l]] = rd.read_uint16()

    gmr['challenges'] = OrderedDict()
    for i in range(0, CHALLENGE_MAX):
        completed_by = rd.read_uint8()
        if not completed_by:
            continue
        gmr['challenges_completed'] += 1

        gmr['challenges'][i] = OrderedDict()
        gmr['challenges'][i]['completed_by'] = completed_by
        gmr['challenges'][i]['completed_time'] = round(rd.read_fixed32(), 2)
        gmr['challenges'][i]['order'] = rd.read_uint8()

        gmr['challenges'][i]['buttons_by'] = OrderedDict()
        for l in range(0, CHALLENGE_BUTTON_MAX):
            gmr['challenges'][i]['buttons_by'][l] = rd.read_uint8()

        gmr['challenges'][i]['powerups_by'] = OrderedDict()
        for l in range(0, CHALLENGE_POWERUP_MAX):
            gmr['challenges'][i]['powerups_by'][l] = rd.read_uint8()

    return gmr


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


class GeneralSection(OrderedDict):
    def __init__(self):
        OrderedDict.__init__(self)
        self.setdefault('game_title', None)
        self.setdefault('game_speed', None)
        self.setdefault('elapsed_game_loops', None)
        self.setdefault('elapsed_game_time', None)
        self.setdefault('elapsed_real_time', None)
        self.setdefault('player_slots', [])

    def addMetadata(self, metadata):
        for row in metadata['Players']:
            self['player_slots'].append(OrderedDict(
                player_id=row['PlayerID'],
                apm=row['APM'],
            ))

    def addHeader(self, header):
        self['elapsed_game_loops'] = header['m_elapsedGameLoops']

    def addDetails(self, details):
        self['game_title'] = details['m_title']
        self['game_speed'] = GAME_SPEED_MAP[details['m_gameSpeed']]
        self['elapsed_game_time'] = round(self['elapsed_game_loops'] * 16.0, 2)
        self['elapsed_real_time'] = round(self['elapsed_game_time'] * (1 + ((details['m_gameSpeed'] - 2) / 5.0)), 2)

        for row in details['m_playerList']:
            pslot = self['player_slots'][row['m_workingSetSlotId']]
            pslot['type'] = PLAYER_TYPE_MAP[row['m_control']]
            if row['m_control'] == 2:
                pslot['handle'] = '%d-S2-%d-%d' % (row['m_toon']['m_region'], row['m_toon']['m_realm'], row['m_toon']['m_id'])
            else:
                pslot['handle'] = None
                pslot['name'] = row['m_name']
            pslot['color'] = OrderedDict()
            pslot['color']['r'] = row['m_color']['m_r']
            pslot['color']['g'] = row['m_color']['m_g']
            pslot['color']['b'] = row['m_color']['m_b']
            pslot['color']['a'] = row['m_color']['m_a']

    def addInitData(self, initd):
        for i, row in enumerate(initd['m_syncLobbyState']['m_userInitialData']):
            if i >= len(self['player_slots']):
                break
            pslot = self['player_slots'][i]
            if row['m_name']:
                pslot['name'] = row['m_name']
            pslot['clan'] = row['m_clanTag'] if row['m_clanTag'] else None


def main():
    archive = mpyq.MPQArchive(sys.argv[1])

    def read_contents(archive, content):
        contents = archive.read_file(content)
        if not contents:
            print('Error: Archive missing {}'.format(content), file=sys.stderr)
            sys.exit(1)
        return contents

    # HEADER
    contents = archive.header['user_data_header']['content']
    header = versions.latest().decode_replay_header(contents)

    # The header's baseBuild determines which protocol to use
    baseBuild = header['m_version']['m_baseBuild']
    try:
        protocol = versions.build(baseBuild)
    except Exception, e:
        print('Unsupported base build: {0} ({1})'.format(baseBuild, str(e)), file=sys.stderr)
        protocol = versions.latest()
        print('Attempting to use newest possible instead: %s' % protocol.__name__, file=sys.stderr)

    metadata = json.loads(read_contents(archive, 'replay.gamemetadata.json'))
    details = protocol.decode_replay_details(read_contents(archive, 'replay.details'))
    initd = protocol.decode_replay_initdata(read_contents(archive, 'replay.initData'))

    general = GeneralSection()
    game_result = None
    
    general.addMetadata(metadata)
    general.addHeader(header)
    general.addDetails(details)
    general.addInitData(initd)

    tevents = protocol.decode_replay_tracker_events(read_contents(archive, 'replay.tracker.events'))

    CV_NAMES = [
        'Ice Baneling Escape - Cold Voyage',
        'Ice Baneling Escape - EZ',
        'Ice Baneling Escape - Pro',
    ]
    if general['game_title'] in CV_NAMES:
        for payload in fetch_payloads_from_tracker(tevents):
            game_result = decode_game_result(payload)
            if game_result['escaped']:
                break

    osects = OrderedDict()
    osects['general'] = general
    osects['result'] = game_result
    print(json.dumps(osects, indent=4, sort_keys=False))


if __name__ == '__main__':
    main()

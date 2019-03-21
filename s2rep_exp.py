#!/usr/bin/python2
# -*- coding: utf-8 -*-

from __future__ import print_function
import sys
import os
import json
from collections import OrderedDict
import mpyq
from s2protocol import versions
import hashlib


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


def seek_payload_in_tracker(tracker):
    for x in tracker:
        if x['_event'] == 'NNet.Replay.Tracker.SUnitBornEvent' and x['m_unitTypeName'] in ['__', 'ShapeTorus4']:
            yield x


def fetch_dstream_from_tracker(tracker, initial_event):
    curr_payload = []
    for x in tracker:
        if x['_event'] == 'NNet.Replay.Tracker.SUnitDiedEvent' and x['m_unitTagIndex'] == initial_event['m_unitTagIndex']:
            return curr_payload
        elif x['_event'] in ['NNet.Replay.Tracker.SUnitBornEvent', 'NNet.Replay.Tracker.SUnitDiedEvent']:
            curr_payload.append(x['m_x'])
            curr_payload.append(x['m_y'])
    raise Exception('unexpected end of dstream')


def decode_game_result(dstream, player_slots):
    MAX_PLAYERS = 16
    ABIL_MAX = 12
    CHALLENGE_MAX = 30
    CHALLENGE_POWERUP_MAX = 16
    CHALLENGE_BUTTON_MAX = 16
    CURRENT_SCHEMA_VERSION = 9

    ABIL_MAP = [
        "BOOST",
        "CREEP",
        "THROW_ESSENCE",
        "ART",
        "SHADE_CREATE",
        "SHADE_USE",
        "THROW_ESSENCE_REVIVE",
        "ART_REVIVE",
        "TIME_SHIFT"
    ]

    rd = DReader(dstream)
    gmr = OrderedDict()

    gmr['schema_version'] = rd.read_uint16()
    if gmr['schema_version'] > CURRENT_SCHEMA_VERSION:
        raise Exception('not supported schema version %d' % gmr['schema_version'])
    if gmr['schema_version'] < 2:
        CHALLENGE_POWERUP_MAX = 8
        CHALLENGE_BUTTON_MAX = 8
    if gmr['schema_version'] < 8:
        MAX_PLAYERS = 11
        ABIL_MAX = 8

    if gmr['schema_version'] >= 9:
        gmr['schema_build_revision'] = rd.read_uint8()
    else:
        gmr['schema_build_revision'] = 0

    if gmr['schema_version'] >= 7:
        gmr['framework_version'] = rd.read_uint16()
    else:
        gmr['framework_version'] = 0
    gmr['game_version'] = rd.read_uint16()
    if gmr['schema_version'] >= 8:
        # IBE-CV speed either 2 (norm) or 4 (faster), for some unkown reason - it should be instead 3 (fast)
        gmr['game_diff'] = rd.read_uint8()
        gmr['game_speed'] = (3 if rd.read_uint8() == 4 else 2)
    elif gmr['schema_version'] >= 6:
        # BTB = diff always 0; speed code relative to normal
        gmr['game_diff'] = rd.read_uint8() + 1
        gmr['game_speed'] = rd.read_uint8() + 2
    else:
        gmode_code = rd.read_uint16()
        gmr['game_speed'] = (1 if gmode_code & (1 << 1) else 0) + 2
        gmr['game_diff'] = (1 if gmode_code & (1 << 0) else 0) + 1
    gmr['escape_time'] = round(rd.read_fixed32(), 2)
    if gmr['schema_version'] >= 3:
        gmr['started_at_rt'] = round(rd.read_fixed32(), 2)
    if gmr['schema_version'] >= 4:
        gmr['started_at_gt'] = round(rd.read_fixed32(), 2)
    gmr['escaped'] = gmr['escape_time'] > 0.0
    rd.read_uint8()
    gmr['challenges_completed'] = 0
    gmr['challenges_total'] = rd.read_uint8()

    gmr['players'] = OrderedDict()
    for i in range(1, MAX_PLAYERS):
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
            if l >= len(ABIL_MAP):
                rd.read_uint16()
                continue
            gmr['players'][i]['abilities_used'][ABIL_MAP[l]] = rd.read_uint16()

    for ps in player_slots:
        if ps['player_id'] not in gmr['players']:
            i = ps['player_id']
            gmr['players'][i] = OrderedDict()
            gmr['players'][i]['left'] = True
            gmr['players'][i]['level'] = None
            gmr['players'][i]['deaths'] = None
            gmr['players'][i]['revives'] = None

            gmr['players'][i]['abilities_used'] = OrderedDict()
            for l in range(0, ABIL_MAX):
                if l >= len(ABIL_MAP):
                    continue
                gmr['players'][i]['abilities_used'][ABIL_MAP[l]] = None

    gmr['challenges'] = OrderedDict()
    for i in range(0, CHALLENGE_MAX):
        completed_by_tmp = rd.read_uint8()
        if not completed_by_tmp:
            continue
        gmr['challenges_completed'] += 1
        gmr['challenges'][i] = OrderedDict()

        gmr['challenges'][i]['completed_by'] = []
        if gmr['schema_version'] < 9:
            if gmr['schema_version'] >= 5:
                gmr['challenges'][i]['completed_by'].append([completed_by_tmp, rd.read_uint8()])
            else:
                gmr['challenges'][i]['completed_by'].append([completed_by_tmp, completed_by_tmp])
        else:
            for l in range(0, completed_by_tmp):
                gmr['challenges'][i]['completed_by'].append([rd.read_uint8(), rd.read_uint8()])

        if gmr['schema_version'] >= 2:
            gmr['challenges'][i]['time_offset_start'] = round(rd.read_fixed32(), 2)
        gmr['challenges'][i]['completed_time'] = round(rd.read_fixed32(), 2)
        gmr['challenges'][i]['order'] = rd.read_uint8()

        # bug in schema v2 (IBE2.1 wouldn't reset timer for the first challenge after restart)
        if gmr['schema_version'] == 2 and gmr['challenges'][i]['order'] == 0:
            gmr['challenges'][i]['time_offset_start'] = 0.0

        gmr['challenges'][i]['buttons_by'] = []
        for l in range(0, CHALLENGE_BUTTON_MAX):
            tmp = [rd.read_uint8()]
            if gmr['schema_version'] >= 5:
                tmp.append(rd.read_uint8())
            else:
                tmp.append(tmp[0])
            if tmp[0] > 0:
                gmr['challenges'][i]['buttons_by'].append(tmp)
            if gmr['schema_version'] >= 3 and tmp[0] == 0:
                break

        gmr['challenges'][i]['powerups_by'] = []
        for l in range(0, CHALLENGE_POWERUP_MAX):
            tmp = [rd.read_uint8()]
            if gmr['schema_version'] >= 5:
                tmp.append(rd.read_uint8())
            else:
                tmp.append(tmp[0])
            if tmp[0] > 0:
                gmr['challenges'][i]['powerups_by'].append(tmp)
            if gmr['schema_version'] >= 3 and tmp[0] == 0:
                break

    # add in team summary
    gmr['team'] = OrderedDict()
    gmr['team']['deaths'] = 0
    gmr['team']['revives'] = 0
    gmr['team']['times_leveled_up'] = 0
    gmr['team']['bonus_levelups'] = 0

    for i in gmr['players']:
        if not gmr['players'][i]['deaths']:
            continue
        gmr['team']['deaths'] += gmr['players'][i]['deaths']
        gmr['team']['revives'] += gmr['players'][i]['revives']
        gmr['team']['times_leveled_up'] += gmr['players'][i]['level'] -  1

    for i in gmr['challenges']:
        for x in gmr['challenges'][i]['powerups_by']:
            gmr['team']['bonus_levelups'] += 1

    # print("[%d/%d]" % (rd.offset, len(rd.buff)))

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
        self.setdefault('timestamp', None)
        self.setdefault('client_version', None)
        self.setdefault('author_handle', None)
        self.setdefault('battle_net', None)
        self.setdefault('server_region', None)
        self.setdefault('resumed_replay', None)
        self.setdefault('player_slots', [])

    def addMetadata(self, metadata):
        self['elapsed_real_time'] = metadata['Duration']

    def addHeader(self, header):
        self['elapsed_game_loops'] = header['m_elapsedGameLoops']
        self['client_version'] = '.'.join([
            str(header['m_version']['m_major']),
            str(header['m_version']['m_minor']),
            str(header['m_version']['m_revision']),
            str(header['m_version']['m_build']),
        ])

    def addDetails(self, details):
        self['game_title'] = details['m_title']
        self['game_speed'] = GAME_SPEED_MAP[details['m_gameSpeed']]
        self['elapsed_game_time'] = round(self['elapsed_game_loops'] / 16.0)
        self['timestamp'] = (details['m_timeUTC'] / 10000000) - 11644473600

    def setupPlayers(self, initd, details, tracker, metadata=None):
        slots = {}
        working_slots = {}

        for slot_id, row in enumerate(initd['m_syncLobbyState']['m_lobbyState']['m_slots']):
            # if slot is FREE (0) or NOT AVAILABLE (1)
            if row['m_control'] <= 1:
                continue

            pslot = OrderedDict(
                player_id=slot_id + 1,
                apm=None,
            )
            self['player_slots'].append(pslot)
            slots[slot_id] = pslot
            working_slots[row['m_workingSetSlotId']] = pslot

            if row['m_userId'] is not None:
                user_data = initd['m_syncLobbyState']['m_userInitialData'][row['m_userId']]
                pslot['name'] = user_data['m_name']
                pslot['clan'] = user_data['m_clanTag']
                pslot['user_id'] = row['m_userId']
            else:
                pslot['name'] = None
                pslot['clan'] = None
                pslot['user_id'] = None

        for row in details['m_playerList']:
            pslot = working_slots[row['m_workingSetSlotId']]
            pslot['type'] = PLAYER_TYPE_MAP[row['m_control']]
            if row['m_control'] == 2:
                pslot['handle'] = '%d-S2-%d-%d' % (row['m_toon']['m_region'], row['m_toon']['m_realm'], row['m_toon']['m_id'])
                pslot['toon'] = {
                    'region': row['m_toon']['m_region'],
                    'realm': row['m_toon']['m_realm'],
                    'id': row['m_toon']['m_id'],
                }
            else:
                pslot['handle'] = None
                pslot['name'] = row['m_name']
            pslot['color'] = OrderedDict()
            pslot['color']['r'] = row['m_color']['m_r']
            pslot['color']['g'] = row['m_color']['m_g']
            pslot['color']['b'] = row['m_color']['m_b']
            pslot['color']['a'] = row['m_color']['m_a']

        for ev in tracker:
            if ev['_event'] != 'NNet.Replay.Tracker.SPlayerSetupEvent':
                break
            if ev['m_slotId'] is None:
                continue
            pslot = slots[ev['m_slotId']]
            pslot['player_id'] = ev['m_playerId']

        if metadata:
            for i, row in enumerate(metadata['Players']):
                self['player_slots'][i]['apm'] = row['APM']

    def addInitData(self, initd):
        self['battle_net'] = initd['m_syncLobbyState']['m_gameDescription']['m_gameOptions']['m_battleNet']
        self['author_handle'] = initd['m_syncLobbyState']['m_gameDescription']['m_mapAuthorName']
        if self['author_handle']:
            region = int(self['author_handle'].split('-')[0])
        else:
            # test mode
            region = 0
        self['server_region'] = {
            'id': region,
            'name': [None, 'NA', 'EU', 'Asia', None, 'CN', 'SEA'][region]
        }

    def processGameEvents(self, gameevents):
        self['resumed_replay'] = False
        for ev in gameevents:
            if ev['_event'] == 'NNet.Game.SHijackReplayGameEvent':
                self['resumed_replay'] = True
                break


class MapInfoSection(OrderedDict):
    def __init__(self):
        OrderedDict.__init__(self)
        self.setdefault('id', None)
        self.setdefault('name', None)


TORUS_LIST = ['ShapeTorus2', 'ShapeTorus22', 'ShapeTorus222']


def torus_to_integer(stream, base=3):
    value = 0;

    for i, uname in enumerate(stream):
        value += TORUS_LIST.index(uname) * pow(base, i);

    return value;


IBE_VER_DELTA1 = 1
IBE_VER_DELTA2 = 2
IBE_VER_DELTA_RIBE = 3

def process_ibe(tracker, map_id, initial_event, player_slots):
    rows = [[]]
    score = {}
    for x in tracker:
        if x['_event'] == 'NNet.Replay.Tracker.SPlayerStatsEvent':
            score[x['m_playerId']] = x
        elif x['_event'] == 'NNet.Replay.Tracker.SUnitBornEvent':
            if x['m_unitTypeName'] in TORUS_LIST:
                rows[-1].append(x['m_unitTypeName'])
            elif x['m_unitTypeName'] == 'ShapeTorus3':
                rows.append([])

    result = {}
    result['escaped'] = True

    result['players'] = {}
    num_left = 0
    for ps in player_slots:
        result['players'][ps['player_id']] = {
            'left': True
        }
    for pid in score:
        result['players'][pid] = {
            'left': score[pid]['m_stats']['m_scoreValueVespeneCurrent'] == 0
        }
        num_left += 1
    # if data indicates that every player has left then it must be incorrect..
    # (early versions of IBE didn't export player status info)
    if num_left == len(result['players']):
        for pid in score:
            result['players'][pid] = {
                'left': False
            }

    rows = map(torus_to_integer, rows)

    if len(rows) == 16:
        dver = IBE_VER_DELTA2
    elif len(rows) == 15:
        dver = IBE_VER_DELTA1
    elif len(rows) == 14:
        if map_id == 'IBE2':
            dver = IBE_VER_DELTA2
        else:
            dver = IBE_VER_DELTA1
    elif len(rows) == 13 and map_id == 'IBE1':
        dver = IBE_VER_DELTA1
    elif len(rows) == 11:
        dver = IBE_VER_DELTA_RIBE
    else:
        raise Exception('unexpected number of rows [%d]: %s' % (len(rows), str(rows)))

    result['escape_time'] = rows.pop(0)
    rows.pop(0)
    difficulty_index = None
    if dver != IBE_VER_DELTA2:
        difficulty_index = rows.pop(0)

    result['team'] = {}
    result['team']['revives'] = rows.pop(0)
    result['team']['deaths'] = rows.pop(0)
    result['team']['bonus_levelups'] = rows.pop(0)
    result['team']['used_power_boost_times'] = rows.pop(0)
    result['team']['used_propel_times'] = rows.pop(0)
    result['team']['used_throw_essence_times'] = rows.pop(0)
    result['team']['used_art_times'] = rows.pop(0)
    result['team']['used_rev_art_times'] = rows.pop(0)
    if dver == IBE_VER_DELTA2:
        result['team']['used_time_shift_times'] = rows.pop(0)
        result['team']['times_leveled_up'] = rows.pop(0)

    if dver != IBE_VER_DELTA_RIBE:
        result['major_version'] = rows.pop(0)
        result['minor_version'] = rows.pop(0)
    else:
        result['major_version'] = None
        result['minor_version'] = None

    if dver == IBE_VER_DELTA2:
        if len(rows):
            difficulty_index = rows.pop(0)
            rows.pop(0)
        else:
            # IBE2 v1.3 - c26ccb8a690e9ced1614de57fe27a255d9fa98ea4ec98ce6cf50cbf973b9b935
            difficulty_index = 1 # assume it's normal/normal
    elif dver == IBE_VER_DELTA1 and len(rows):
        rows.pop(0)
        if len(rows):
            result['escape_time'] += rows.pop(0) / 100.0

    result['game_diff'] = 2 if difficulty_index in [3, 4] else 1
    result['game_speed'] = 4 if difficulty_index in [2, 4] else 2

    return result


def hash_result(general, map_id, result):
    inp = []
    inp.append(map_id)
    inp.append(general['client_version'])
    inp.append(general['author_handle'])

    for x in general['player_slots']:
        inp.append(x['player_id'])
        inp.append(x['type'])
        if x['type'] == 'USER':
            inp.append(x['handle'])
        if result:
            inp.append(result['players'][x['player_id']]['left'])

    if result:
        inp.append(result['escape_time'])
        inp.append(result['team']['deaths'])
        inp.append(result['team']['revives'])
        inp.append(result['team']['bonus_levelups'])

    def to_str(val):
        return str(val)

    return hashlib.sha1(','.join(map(to_str, inp))).hexdigest()


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
    except Exception as e:
        print('Unsupported base build: {0} ({1})'.format(baseBuild, str(e)), file=sys.stderr)
        protocol = versions.latest()
        print('Attempting to use newest possible instead: %s' % protocol.__name__, file=sys.stderr)

    details = protocol.decode_replay_details(read_contents(archive, 'replay.details'))
    initd = protocol.decode_replay_initdata(read_contents(archive, 'replay.initData'))
    try:
        if archive.files.index('replay.gamemetadata.json'):
            metadata = json.loads(read_contents(archive, 'replay.gamemetadata.json'))
    except ValueError:
        metadata = None

    general = GeneralSection()
    map_info = MapInfoSection()
    game_result = None

    general.addHeader(header)
    general.addDetails(details)
    general.addInitData(initd)
    if metadata:
        general.addMetadata(metadata)

    NAME_MAP = {
        'Ice Baneling Escape': 'IBE1',
        '도전! 맹독충의 빙판탈출': 'IBE1', # koKR
        '毒爆大逃亡': 'IBE1', # zhTW

        'Reverse Ice Baneling Escape': 'RIBE1',

        'Ice Baneling Escape 2': 'IBE2',
        '맹독충의 빙판탈출 2': 'IBE2', # koKR

        'Ice Baneling Escape 2.1 - The Ice Awakens': 'IBE2.1',
        '맹독충의 빙판탈출 2.1 - 얼음 깨기': 'IBE2.1', # koKR

        'Ice Baneling Escape - Cold Voyage': 'IBE-CV',
        '맹독충의 빙판탈출 - 차가운 여행': 'IBE-CV', # koKR

        'Ice Baneling Escape - EZ': 'IBE-CV-EZ',
        '맹독충의 빙판탈출 - 차가운 여행 - EZ': 'IBE-CV-EZ', # koKR

        'Ice Baneling Escape - Pro': 'IBE-CV-PRO',
        '맹독충의 빙판탈출 - 차가운 여행 - PRO': 'IBE-CV-PRO', # koKR

        'Back to Brood Ice Escape': 'BTB',
        'Back to Brood Ice Escape PRO': 'BTB-PRO',
        'Back to Brood Ice Escape PRO mode': 'BTB-PRO',
        'Back to Brood Baneling mode': 'BTB-BANE',
    }

    fname = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'minfo.json')
    with open(fname, 'r') as fp:
        minfo = json.load(fp, encoding='utf-8')

    map_id = None
    try:
        map_id = NAME_MAP[general['game_title']]

        # IBE2.1 before name change
        if general['game_title'] == '맹독충의 빙판탈출 2' and general['author_handle'] in ['1-S2-1-4182020', '2-S2-1-2642502', '3-S2-1-4137301']:
            map_id = 'IBE2.1'

        map_info['id'] = map_id
        map_info['name'] = minfo['maps'][map_info['id']]['name']
    except KeyError:
        map_info = None

    if map_info:
        tracker = protocol.decode_replay_tracker_events(read_contents(archive, 'replay.tracker.events'))

        general.setupPlayers(initd, details, tracker, metadata)

        for initial_event in seek_payload_in_tracker(tracker):
            if initial_event['m_unitTypeName'] == '__':
                dstream = fetch_dstream_from_tracker(tracker, initial_event)
                game_result = decode_game_result(dstream, general['player_slots'])
                if game_result['escaped']:
                    break
                else:
                    game_result = None
            else:
                game_result = process_ibe(tracker, map_info['id'], initial_event, general['player_slots'])

        # process gamevents to determine if replay was resumed
        # do so only in case of successful runs
        if game_result:
            gameevents = protocol.decode_replay_game_events(read_contents(archive, 'replay.game.events'))
            general.processGameEvents(gameevents)

    osects = OrderedDict()
    osects['general'] = general
    osects['map'] = map_info
    osects['result'] = game_result
    osects['hash'] = hash_result(general, map_id, game_result)
    print(json.dumps(osects, indent=4, sort_keys=False))


if __name__ == '__main__':
    main()

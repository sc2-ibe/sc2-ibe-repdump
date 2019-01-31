#!/usr/bin/env python2
# -*- coding: utf-8 -*-

from __future__ import print_function
import sys
import os
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
        self.setdefault('timestamp', None)
        self.setdefault('client_version', None)
        self.setdefault('author_handle', None)
        self.setdefault('battle_net', None)
        self.setdefault('server_region', None)
        self.setdefault('player_slots', [])

    def addMetadata(self, metadata):
        self['elapsed_real_time'] = metadata['Duration']

    def addHeader(self, header):
        self['elapsed_game_loops'] = header['m_elapsedGameLoops']
        self['client_version'] = header['m_version']

    def addDetails(self, details):
        self['game_title'] = details['m_title']
        self['game_speed'] = GAME_SPEED_MAP[details['m_gameSpeed']]
        self['elapsed_game_time'] = round(self['elapsed_game_loops'] / 16.0)
        self['timestamp'] = (details['m_timeUTC'] / 10000000) - 11644473600

    def setupPlayers(self, initd, details, tracker, metadata=None):
        slots = {}
        working_slots = {}

        for slot_id, row in enumerate(initd['m_syncLobbyState']['m_lobbyState']['m_slots']):
            if row['m_control'] == 0:
                continue

            pslot = OrderedDict(
                player_id=None,
                apm=None,
            )
            self['player_slots'].append(pslot)
            slots[slot_id] = pslot
            working_slots[row['m_workingSetSlotId']] = pslot

            if row['m_userId'] is not None:
                user_data = initd['m_syncLobbyState']['m_userInitialData'][row['m_userId']]
                pslot['name'] = user_data['m_name']
                pslot['clan'] = user_data['m_clanTag']
            else:
                pslot['name'] = None
                pslot['clan'] = None

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
                break
            pslot = slots[ev['m_slotId']]
            pslot['player_id'] = ev['m_playerId']

        if metadata:
            for i, row in enumerate(metadata['Players']):
                self['player_slots'][i]['apm'] = row['APM']

    def addInitData(self, initd):
        self['battle_net'] = initd['m_syncLobbyState']['m_gameDescription']['m_gameOptions']['m_battleNet']
        self['author_handle'] = initd['m_syncLobbyState']['m_gameDescription']['m_mapAuthorName']
        region = int(self['author_handle'].split('-')[0])
        self['server_region'] = {
            'id': region,
            'name': [None, 'NA', 'EU', 'Asia', None, 'CN', 'SEA'][region]
        }
        

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

def process_ibe(tracker):
    past_torus4 = False
    rows = []
    score = {}
    
    for x in tracker:
        if past_torus4:
            if x['_eventid'] == 0:
                score[x['m_playerId']] = x
            elif x['_eventid'] == 1:
                if x['m_unitTypeName'] in TORUS_LIST:
                    rows[-1].append(x['m_unitTypeName'])
                elif x['m_unitTypeName'] == 'ShapeTorus3':
                    rows.append([])
        else:
            if x['_eventid'] == 1 and x['m_unitTypeName'] == 'ShapeTorus4':
                past_torus4 = True
                rows.append([])

    if not past_torus4:
        return None

    result = {}
    result['escaped'] = True

    result['players'] = {}
    for pid in score:
        result['players'][pid] = {
            'left': score[pid]['m_stats']['m_scoreValueVespeneCurrent'] == 0
        }
    
    rows = map(torus_to_integer, rows)
    
    if len(rows) == 16:
        dver = IBE_VER_DELTA2
    elif 14 <= len(rows) and len(rows) <= 15:
        dver = IBE_VER_DELTA1
    elif len(rows) == 11:
        dver = IBE_VER_DELTA_RIBE
    else:
        raise Exception('unexpected number of rows [%d]: %s' % (len(rows), str(rows)))
    
    result['escape_time'] = rows.pop(0)
    rows.pop(0)
    if dver != IBE_VER_DELTA2:
        result['difficulty_index'] = rows.pop(0)
    
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
        result['difficulty_index'] = rows.pop(0)
        rows.pop(0)
    elif dver == IBE_VER_DELTA1:
        rows.pop(0)
        if len(rows):
            result['escape_time'] += rows.pop(0) / 100.0

    return result


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

        'Ice Baneling Escape - Cold Voyage': 'IBE-CV',
        'Ice Baneling Escape - EZ': 'IBE-CV-EZ',
        'Ice Baneling Escape - Pro': 'IBE-CV-PRO',
    }
    
    fname = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'minfo.json')
    with open(fname, 'r') as fp:
        minfo = json.load(fp, encoding='utf-8')

    try:
        map_id = NAME_MAP[general['game_title']]
        map_info['id'] = map_id
        map_info['name'] = minfo['maps'][map_info['id']]['name']
    except KeyError:
        map_info = None

    if map_info:
        tracker = protocol.decode_replay_tracker_events(read_contents(archive, 'replay.tracker.events'))
        general.setupPlayers(initd, details, tracker, metadata)
        
        if map_info['id'] in ['IBE-CV', 'IBE-CV-EZ', 'IBE-CV-PRO']:
            for payload in fetch_payloads_from_tracker(tracker):
                game_result = decode_game_result(payload)
                if game_result['escaped']:
                    break
        elif map_info['id'] in ['IBE1', 'RIBE1', 'IBE2', 'IBE2.1']:
            game_result = process_ibe(tracker)
        else:
            raise Exception('unknown map id "%s"' % map_info['id'])
    
    osects = OrderedDict()
    osects['general'] = general
    osects['map'] = map_info
    osects['result'] = game_result
    print(json.dumps(osects, indent=4, sort_keys=False))


if __name__ == '__main__':
    main()

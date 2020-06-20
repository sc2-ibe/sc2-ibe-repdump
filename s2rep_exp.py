#!/usr/bin/python2
# -*- coding: utf-8 -*-

from __future__ import print_function
from pprint import pprint, pformat, saferepr
import sys
import os
import json
import copy
from collections import OrderedDict
import mpyq
from s2protocol import versions
import sc2reader
import hashlib
import argparse
import logging
from s2ibedump.evaluation import GameEvaluation
from s2ibedump.objects import PLAYER_TYPE_MAP, GAME_SPEED_MAP, PlayerSlot
from s2ibedump.helpers import getPlayerSlot, toJson
from s2ibedump.s2map import KNOWN_NAME_MAP


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
    CURRENT_SCHEMA_VERSION = 10

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

    if gmr['schema_version'] >= 10:
        gmr['framework_version'] = rd.read_uint32()
    elif gmr['schema_version'] >= 7:
        gmr['framework_version'] = rd.read_uint16()
    else:
        gmr['framework_version'] = 0

    if gmr['schema_version'] >= 10:
        gmr['game_version'] = rd.read_uint32()
    else:
        gmr['game_version'] = rd.read_uint16()

    if gmr['schema_version'] >= 8:
        gmr['game_diff'] = rd.read_uint8()
        gmr['game_speed'] = rd.read_uint8()
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

    logging.debug('[%d/%d]' % (rd.offset, len(rd.buff)))

    return gmr


def fix_game_result(map_id, gmr):
    # IBE-CV: versions before introduction of speedy incorrectly reported `Fast` as a value of `4` (no idea why)
    if map_id.startswith('IBE-CV') and gmr['framework_version'] <= 22 and gmr['game_speed'] == 4:
        gmr['game_speed'] = 3

    # Delta IBE
    if map_id in ['IBE1', 'RIBE1', 'IBE2']:
        # it uses major.minor versioning
        # major version is kept at 1, so we're not going to care about it (unless it'll change from 1)
        major_ver = gmr['game_version'] >> 16
        minor_ver = gmr['game_version'] & 0xFFFF
        if major_ver == 1:
            major_ver = 0
        gmr['game_version'] = (major_ver << 16) | (minor_ver & 0xFFFF)


    return gmr


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

    def setupPlayers(self, initd, details):
        lobby_slots = {}
        working_slots = {}

        for slot_id, row in enumerate(initd['m_syncLobbyState']['m_lobbyState']['m_slots']):
            # if slot is FREE (0) or NOT AVAILABLE (1)
            if row['m_control'] <= 1:
                continue

            pslot = PlayerSlot()
            self['player_slots'].append(pslot)
            pslot.slot_id = slot_id
            pslot.player_id = slot_id + 1
            lobby_slots[slot_id] = pslot
            working_slots[row['m_workingSetSlotId']] = pslot
            pslot.is_human = False
            pslot.is_observer = False

            if row['m_userId'] is not None:
                user_data = initd['m_syncLobbyState']['m_userInitialData'][row['m_userId']]
                pslot.name = user_data['m_name']
                pslot.clan = user_data['m_clanTag']
                pslot.user_id = row['m_userId']

        for row in details['m_playerList']:
            pslot = working_slots[row['m_workingSetSlotId']]
            pslot.type = PLAYER_TYPE_MAP[row['m_control']]
            if row['m_control'] == 2:
                pslot.handle = '%d-S2-%d-%d' % (row['m_toon']['m_region'], row['m_toon']['m_realm'], row['m_toon']['m_id'])
                pslot.toon = {
                    'region': row['m_toon']['m_region'],
                    'realm': row['m_toon']['m_realm'],
                    'id': row['m_toon']['m_id'],
                }
            else:
                pslot.name = row['m_name']
            pslot.color = OrderedDict()
            pslot.color['r'] = row['m_color']['m_r']
            pslot.color['g'] = row['m_color']['m_g']
            pslot.color['b'] = row['m_color']['m_b']
            pslot.color['a'] = row['m_color']['m_a']

    def addInitData(self, initd):
        self['battle_net'] = bool(initd['m_syncLobbyState']['m_gameDescription']['m_gameOptions']['m_battleNet'])
        self['author_handle'] = initd['m_syncLobbyState']['m_gameDescription']['m_mapAuthorName']
        if self['author_handle']:
            self['server_region'] = int(self['author_handle'].split('-')[0])
        else:
            # test mode
            self['server_region'] = 0

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
        if result['players'][pid]['left']:
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

    if map_id == 'IBE2':
        result['game_diff'] = 2 if difficulty_index in [3, 4] else 1
    else:
        result['game_diff'] = 1 # there's only normal diff in IBE1
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
        if 'team' in result:
            inp.append(result['team']['deaths'])
            inp.append(result['team']['revives'])
            inp.append(result['team']['bonus_levelups'])

    def to_str(val):
        return str(val)

    return hashlib.sha1(','.join(map(to_str, inp))).hexdigest()


def mergeEscapeResults(firstResult, secondResult):
    gameResult = copy.deepcopy(secondResult)

    # use time of first escape, without including time spent on bonus maps
    gameResult['escape_time'] = firstResult['escape_time']
    gameResult['total_time'] = secondResult['escape_time']

    # don't care about players who left before bonus maps
    for pkey in gameResult['players']:
        gameResult['players'][pkey]['left'] = firstResult['players'][pkey]['left']

    return gameResult


class ExitCodes(object):
    INTERNAL_ERROR = 1
    NOT_SUPPORTED = 2


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('replay_file', help='.SC2Replay file to load')
    parser.add_argument('-v', '--verbose', help='verbose logging', action='store_true')
    parser.add_argument('--map-id', help='Force replay to be handled as it was played on a given map-id')
    parser.add_argument('--include-loss', help='include results that did not end with an escape', action='store_true')
    parser.add_argument('--game-speed', type=int, default=4)
    parser.add_argument('--evaluate', action='store_true')
    parser.add_argument('--allow-offline', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(
        format='%(asctime)s,%(msecs)-3d %(levelname)s: %(message)s',
        datefmt='%H:%M:%S'
    )
    logging._levelNames[logging.DEBUG] = 'DEBG'
    logging._levelNames[logging.WARNING] = 'WARN'
    logging._levelNames[logging.ERROR] = 'ERRO'
    logging._levelNames[logging.CRITICAL] = 'CRIT'
    logging.addLevelName(logging.DEBUG, "\033[1;35m%s\033[1;0m" % logging.getLevelName(logging.DEBUG))
    logging.addLevelName(logging.INFO, "\033[1;32m%s\033[1;0m" % logging.getLevelName(logging.INFO))
    logging.addLevelName(logging.WARNING, "\033[1;33m%s\033[1;0m" % logging.getLevelName(logging.WARNING))
    logging.addLevelName(logging.ERROR, "\033[1;31m%s\033[1;0m" % logging.getLevelName(logging.ERROR))
    logging.addLevelName(logging.CRITICAL, "\033[1;41m%s\033[1;0m" % logging.getLevelName(logging.CRITICAL))

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.ERROR)


    try:
        archive = mpyq.MPQArchive(args.replay_file, listfile=False)
    except ValueError as e:
        if e.message == 'Invalid file header.':
            logging.error(e.message)
            sys.exit(ExitCodes.NOT_SUPPORTED)

    def read_contents(archive, content):
        contents = archive.read_file(content)
        if not contents:
            logging.critical('Archive missing file: "%s"' % content)
            sys.exit(ExitCodes.NOT_SUPPORTED)
        return contents

    # HEADER
    contents = archive.header['user_data_header']['content']
    header = versions.latest().decode_replay_header(contents)

    # The header's baseBuild determines which protocol to use
    baseBuild = header['m_version']['m_baseBuild']
    logging.debug('Protocol build: %s' % baseBuild)
    try:
        protocol = versions.build(baseBuild)
    except Exception as e:
        logging.warn('Unsupported protocol: (%s)' % str(e))
        if baseBuild > 32283 and baseBuild < 51702:
            protocol = versions.build(51702)
        # in case there are some holes in releases of s2protocol between 51702 and 70154
        # don't even attempt to decode so problem can be investigated
        elif baseBuild < 70154:
            sys.exit(ExitCodes.INTERNAL_ERROR)
        # since 70154 things are stable so fallback to newest available if there's no direct match
        else:
            protocol = versions.latest()
        logging.warn('Attempting to use %s instead' % protocol.__name__)

    details = protocol.decode_replay_details(read_contents(archive, 'replay.details'))
    if len(details['m_cacheHandles']) == 0 and not args.allow_offline:
        logging.critical('Test mode detected - map was run from editor')
        sys.exit(ExitCodes.NOT_SUPPORTED)

    archive_files = archive.read_file('(listfile)').decode('ascii').splitlines()
    try:
        archive_files.index('replay.tracker.events')
    except ValueError:
        logging.critical('"%s" missing' % 'replay.tracker.events')
        sys.exit(ExitCodes.NOT_SUPPORTED)

    # tracker section should be present in replays from build 25604 and above
    # May 7th 2013
    # https://liquipedia.net/starcraft2/Patch_2.0.8
    if protocol.tracker_eventid_typeid is None:
        logging.critical('"protocol.tracker_eventid_typeid" missing')
        protocol.decode_replay_tracker_events = versions.build(26490).decode_replay_tracker_events

    s2rep = None
    try:
        s2rep = sc2reader.load_replay(args.replay_file, load_level=2)
    except Exception as e:
        import traceback
        if args.allow_offline:
            logging.warn(traceback.format_exc())
            logging.warn('sc2reader failed, falling back to legacy reader..')
        else:
            logging.error(traceback.format_exc())
            sys.exit(ExitCodes.INTERNAL_ERROR)

    try:
        if archive_files.index('replay.gamemetadata.json'):
            metadata = json.loads(read_contents(archive, 'replay.gamemetadata.json'))
    except ValueError:
        metadata = None

    general = GeneralSection()
    map_info = MapInfoSection()
    game_result = None

    logging.info('Building general section..')
    general.addHeader(header)
    general.addDetails(details)

    if s2rep:
        general['author_handle'] = s2rep.raw_data['replay.initData']['game_description']['map_author_name']
        general['server_region'] = int(general['author_handle'].split('-')[0])
        general['battle_net'] = bool(s2rep.raw_data['replay.initData']['game_description']['game_options']['battle_net'])
        general['player_slots'] = []
        for p in s2rep.players:
            general['player_slots'].append(PlayerSlot.fromParticipant(p))
        for p in s2rep.observers:
            general['player_slots'].append(PlayerSlot.fromObserver(p))
    else:
        logging.info('Setting up players..')
        initd = protocol.decode_replay_initdata(read_contents(archive, 'replay.initData'))
        general.addInitData(initd)
        general.setupPlayers(initd, details)

    fname = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'minfo.json')
    with open(fname, 'r') as fp:
        minfo = json.load(fp, encoding='utf-8')

    map_id = None
    try:
        if args.map_id is not None:
            map_id = args.map_id
        else:
            map_id = KNOWN_NAME_MAP[general['game_title']]

        # IBE2.1 before name change
        if general['game_title'] == '맹독충의 빙판탈출 2' and general['author_handle'] in ['1-S2-1-4182020', '2-S2-1-2642502', '3-S2-1-4137301']:
            map_id = 'IBE2.1'

        map_info['id'] = map_id
        map_info['name'] = minfo['maps'][map_info['id']]['name']
    except KeyError:
        map_info = None
        logging.info('Unknown map title: "%s"' % (general['game_title']))

    # IBE-CV Patch: Faster changed to Fast - 31 July 2017
    # https://gitlab.com/sc2-ibe/sef/commit/d9f2fe206748daf55270e7a2d108358736aa079f
    # SC2 Patch: 3.17.1.57218 - 7 September 2017
    if map_info and map_info['id'].startswith('IBE-CV') and baseBuild < 57218:
        logging.critical('Old IBE-CV not supported')
        sys.exit(ExitCodes.NOT_SUPPORTED)

    # BTB support added on 24 February 2019
    # https://gitlab.com/sc2-ibe/btb/commit/3e3ec2e0a049b1a0d0bbbf3217dbcaf96aa96708
    # Patch 4.8.3.72282 - 19 February 2019
    # Patch 4.8.2.71663 - 22 January 2019
    if map_info and map_info['id'].startswith('BTB') and baseBuild < 71663:
        logging.critical('Old BTB not yet supported')
        sys.exit(ExitCodes.INTERNAL_ERROR)

    results_all = []
    deltaResult = None
    sefResult = None
    if map_info:
        logging.debug('Reading tracker events..')
        tracker = protocol.decode_replay_tracker_events(read_contents(archive, 'replay.tracker.events'))

        # fix player_id
        trackerPlayerSetup = False
        for ev in tracker:
            if ev['_event'] != 'NNet.Replay.Tracker.SPlayerSetupEvent':
                break
            trackerPlayerSetup = True
            if ev['m_slotId'] is None:
                continue
            getPlayerSlot(general['player_slots'], slot_id=ev['m_slotId'])['player_id'] = ev['m_playerId']

        if not trackerPlayerSetup:
            logging.warn('SPlayerSetupEvent event not present')
            for item in general['player_slots']:
                item['player_id'] = item['slot_id'] + 1

        # get APM from metadata
        if metadata:
            try:
                for item in metadata['Players']:
                    if 'APM' not in item:
                        continue
                    getPlayerSlot(general['player_slots'], player_id=item['PlayerID'])['apm'] = item['APM']
            except KeyError:
                pass

        logging.info('Seeking game result..')
        for initial_event in seek_payload_in_tracker(tracker):
            logging.debug('Decoding result at gameloop %d' % (initial_event['_gameloop']))
            if initial_event['m_unitTypeName'] == '__':
                dstream = fetch_dstream_from_tracker(tracker, initial_event)
                logging.debug('dstream %s' % (str(dstream)))
                game_result = decode_game_result(dstream, general['player_slots'])
                game_result = fix_game_result(map_info['id'], game_result)
                if args.include_loss:
                    results_all.append(game_result)
                if game_result['escaped']:
                    # there might be a "secon escape" in case of BTB - after completing bonus maps
                    if sefResult:
                        game_result = mergeEscapeResults(sefResult, game_result)
                    sefResult = game_result
                else:
                    game_result = None
            else:
                deltaResult = process_ibe(tracker, map_info['id'], initial_event, general['player_slots'])

        if args.evaluate and map_info['id'] not in ['BTB', 'BTB-PRO', 'BTB-BANE', 'BTB-EZ', 'IBE2.1','Zealot']:
            game_result = None
            doEvaluate = True

            # deltaResult was added in: IBE1 v1.46 released in Oct 30, 2013
            # Patch 2.0.12.26825 - 11 November 2013
            if not deltaResult and baseBuild < 26825 and map_info['id'] == 'IBE1':
                logging.warning('IBE version before deltaResult')

            # don't evaluate non-IBE1 games without deltaResult
            if not deltaResult and map_info['id'] in ['IBE2', 'RIBE1']:
                doEvaluate = False

            # CV support added on 21 January 2019
            # Patch 4.8.2.71663 - 22 January 2019
            if map_info['id'].startswith('IBE-CV'):
                # don't evaluate if it's recent enough version
                if baseBuild >= 71663 and (sefResult is None or sefResult['schema_version'] >= 5):
                    doEvaluate = False
                    game_result = sefResult

            if doEvaluate or (not game_result and args.include_loss):
                defaultGameSpeed = args.game_speed
                if deltaResult is not None:
                    defaultGameSpeed = deltaResult['game_speed']
                gstate = GameEvaluation(
                    baseBuild,
                    map_info['id'],
                    general['player_slots'],
                    protocol.decode_replay_tracker_events(read_contents(archive, 'replay.tracker.events')),
                    protocol.decode_replay_game_events(read_contents(archive, 'replay.game.events')),
                    defaultGameSpeed
                )
                for session in gstate.process():
                    if not session:
                        continue
                    game_result = gstate.rebuildGameResult(deltaResult=deltaResult, sefResult=sefResult)
                    results_all.append(game_result)
                    if game_result['escaped']:
                        break
                    else:
                        game_result = None
                general['resumed_replay'] = bool(gstate.hijackReplayGameEvent)

        # process gamevents to determine if replay was resumed
        # do so only in case of successful runs
        if game_result and general['resumed_replay'] is None:
            logging.info('Processing game events..')
            gameevents = protocol.decode_replay_game_events(read_contents(archive, 'replay.game.events'))
            general.processGameEvents(gameevents)

    osects = OrderedDict()
    osects['general'] = general
    osects['map'] = map_info
    osects['result'] = game_result
    if args.include_loss:
        osects['results'] = results_all
    if deltaResult:
        osects['delta_result'] = deltaResult
    if sefResult:
        osects['sef_result'] = sefResult

    if deltaResult is not None:
        osects['hash'] = hash_result(general, map_id, deltaResult)
    elif sefResult is not None:
        osects['hash'] = hash_result(general, map_id, sefResult)
    else:
        osects['hash'] = hash_result(general, map_id, game_result)

    print(toJson(osects))

    # CN
    if not map_info and general['server_region'] == 5:
        logging.info('CN server')
        sys.exit(ExitCodes.INTERNAL_ERROR)


if __name__ == '__main__':
    main()

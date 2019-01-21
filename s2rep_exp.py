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
    print('%d: %s' % (len(payload), payload))
    rd = DReader(payload)
    gmr = OrderedDict()

    gmr['schema_version'] = rd.read_uint16()
    gmr['game_version'] = rd.read_uint16();
    gmr['game_code'] = rd.read_uint16()
    gmr['game_speed'] = gmr['game_code'] & (1 << 0)
    gmr['game_diff'] = gmr['game_code'] & (1 << 1)
    gmr['escape_time'] = rd.read_fixed32();
    gmr['escaped'] = gmr['escape_time'] > 0.0
    gmr['challenges_completed'] = rd.read_uint8();
    gmr['challenges_total'] = rd.read_uint8();

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

        gmr['challenges'][i] = OrderedDict()
        gmr['challenges'][i]['completed_by'] = completed_by
        gmr['challenges'][i]['completed_time'] = rd.read_fixed32();
        gmr['challenges'][i]['order'] = rd.read_uint8();

        gmr['challenges'][i]['buttons_by'] = OrderedDict()
        for l in range(0, CHALLENGE_BUTTON_MAX):
            gmr['challenges'][i]['buttons_by'][l] = rd.read_uint8()

        gmr['challenges'][i]['powerups_by'] = OrderedDict()
        for l in range(0, CHALLENGE_POWERUP_MAX):
            gmr['challenges'][i]['powerups_by'][l] = rd.read_uint8()

    print(json.dumps(
        gmr,
        indent=4
    ))
    print('%d / %d' % (rd.offset, len(rd.buff)))


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

    contents = read_contents(archive, 'replay.tracker.events')
    tevents = protocol.decode_replay_tracker_events(contents)

    map(decode_game_result, fetch_payloads_from_tracker(tevents))


if __name__ == '__main__':
    main()

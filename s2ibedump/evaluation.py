#!/usr/bin/python2
# -*- coding: utf-8 -*-

from __future__ import print_function
from pprint import pprint
import logging
import math
from collections import OrderedDict
from .s2map import MapInfo, CmdFlags


class EventStream(object):
    def __init__(self, generator):
        self.generator = generator
        self.empty = False
        self.peek = None
        self.do_peek()

    def __iter__(self):
        return self

    def do_peek(self):
        try:
            self.peek = self.generator.next()
        except StopIteration:
            self.peek = None
            self.empty = True

    def next(self):
        if self.empty:
            raise StopIteration()
        to_return = self.peek
        self.do_peek()
        return to_return


class UnitState(object):
    def __init__(self):
        self.units = {}

    def fetchUnits(self, playerIds=None, unitName=None, createdAt=None, includeRemoved=False):
        r = []
        for key in self.units:
            if self.units[key]['removed'] >= 0 and not includeRemoved:
                continue
            if createdAt is not None and self.units[key]['createdAt'] != createdAt:
                continue
            if playerIds != None and self.units[key]['controlPlayerId'] not in playerIds:
                continue
            if unitName != None and self.units[key]['unitTypeName'] != unitName:
                continue
            r.append(self.units[key])
        return r

    def onEvent(self, ev):
        if ev['_event'] == 'NNet.Replay.Tracker.SUnitBornEvent':
            self.units[ev['m_unitTagIndex']] = {
                'createdAt': ev['_gameloop'],
                'removed': -1,
                'unitTagIndex': ev['m_unitTagIndex'],
                'unitTagRecycle': ev['m_unitTagRecycle'],
                'controlPlayerId': ev['m_controlPlayerId'],
                'unitTypeName': ev['m_unitTypeName'],
                'posX': ev['m_x'],
                'posY': ev['m_y'],
            }
        elif ev['_event'] == 'NNet.Replay.Tracker.SUnitDiedEvent':
            self.units[ev['m_unitTagIndex']]['removed'] = ev['_gameloop']


class GameSession(object):
    def __init__(self):
        self.clear()

    def clear(self):
        self.cLevelId = None
        self.clInitAt = None
        self.clUnits = []
        self.clPowerups = []
        self.gameStartedAt = None
        self.gameEscapedAt = None
        self.levels = OrderedDict()
        self.banelings = OrderedDict()
        self.playerStats = OrderedDict()
        self.abilRawUsage = OrderedDict()
        self.moveOrders = OrderedDict()

    def clearMoveOrders(self):
        for i in range(10):
            self.moveOrders[i + 1] = []

    def createPlayer(self, playerId):
        self.moveOrders[playerId] = []
        self.playerStats[playerId] = {
            'deaths': 0,
            'level': 1,
        }
        self.abilRawUsage[playerId] = OrderedDict()

    def getLivingUnits(self):
        r = []
        for item in self.clUnits:
            if item['removed'] >= 0:
                continue
            r.append(item)
        return r

    def banelingsCount(self):
        r = 0
        for item in self.banelings.values():
            # if item['removed'] >= 0:
            #     continue
            r += 1
        return r

    def registerMoveOrder(self, gameloop, playerId, posX, posY):
        self.moveOrders[playerId].append({
            'gameloop': gameloop,
            'x': posX,
            'y': posY,
        })

    def estimatePlayerPosition(self, playerId, atGameloop, startPos):
        currPos = {
            'x': startPos['x'],
            'y': startPos['y'],
        }
        for morder in self.moveOrders[playerId]:
            if morder['gameloop'] > atGameloop:
                break
            currPos['x'] = morder['x']
            currPos['y'] = morder['y']
        return currPos


class GameEvaluation(object):
    def __init__(self, mapId, playerSlots, trEvents, gmEvents, timeFactor):
        self.mapId = mapId
        self.playerSlots = playerSlots
        self.trEvents = EventStream(trEvents)
        self.gmEvents = EventStream(gmEvents)
        self.timeFactor = timeFactor
        self.gameloop = 0
        self.unState = UnitState()
        self.mapInfo = MapInfo(mapId)
        self.session = GameSession()
        self.playersLeft = {}
        self.userMap = {}
        self.playerMap = {}
        for x in self.playerSlots:
            if not isinstance(x['user_id'], int):
                continue
            self.userMap[x['user_id']] = x
            self.playerMap[x['player_id']] = x

    def next(self):
        trEv = self.trEvents.peek
        gmEv = self.gmEvents.peek
        if trEv and trEv['_gameloop'] <= gmEv['_gameloop']:
            return self.trEvents.next()
        elif gmEv:
            return self.gmEvents.next()
        else:
            raise StopIteration()

    def logGame(self, msg, gameloop=None, userId=None, playerId=None):
        out = ''

        if gameloop == None:
            gameloop = self.gameloop
        secs = gameloop / 16
        out += '%d:%02d:%02d,%d %06d' % (
            secs / 3600,
            secs % 3600 / 60,
            secs % 60,
            (gameloop % 16 / 16.0) * 10,
            gameloop,
        )

        if userId != None:
            out += ' <%s>' % self.userMap[userId]['name']

        if playerId != None:
            out += ' <%s>' % self.playerMap[playerId]['name']

        out += ' %s' % msg

        logging.debug(out)

    def getActivePlayers(self):
        r = []
        for playerId in self.playerMap:
            if playerId in self.playersLeft and self.session.clInitAt > self.playersLeft[playerId]:
                continue
            r.append(playerId)
        return r

    def getPlayersClosest(self, gameloop, targetX, targetY):
        playersPosition = []
        for playerId in self.session.banelings:
            position = self.session.estimatePlayerPosition(playerId, gameloop, self.mapInfo.levelRegions[self.session.cLevelId]['spawn'].getCenter())
            distance = math.hypot(
                position['x'] - targetX,
                position['y'] - targetY
            )
            playersPosition.append({
                'playerId': playerId,
                'distance': distance,
            })

        def closest(item):
            return item['distance']

        playersPosition.sort(key=closest)

        return playersPosition

    def levelCompleted(self, gameloopEnd):
        if self.session.cLevelId == 0:
            completedAt = gameloopEnd
        else:
            completedAt = gameloopEnd
            if self.mapId == 'IBE2':
                completedAt -= (16.0 * 3.0 * self.timeFactor)
            completedAt -= (16.0 * 1.5 * self.timeFactor)

        startedAt = self.session.clInitAt
        if self.mapId == 'IBE1':
            startedAt += 16.0 * 1.0 * self.timeFactor
            if self.session.cLevelId == 0:
                startedAt += 16.0 * 8.0

        ticks = completedAt - startedAt
        secs = ticks / (16.0 * self.timeFactor)
        if self.session.cLevelId in self.session.levels:
            raise Exception('Level %d already completed - missmatch?' % self.session.cLevelId)

        powerupsBy = []
        for item in self.session.clPowerups:
            if item['removed'] == -1 or item['removed'] >= gameloopEnd:
                continue
            playersPosition = self.getPlayersClosest(item['removed'], item['posX'], item['posY'])
            playerId = playersPosition[0]['playerId']
            powerupsBy.append(playerId)
            self.session.playerStats[playerId]['level'] += 1
            self.logGame('Powerup acquired - removedAt=%d %s' % (item['removed'], playersPosition[0]))

        finishCenter = self.mapInfo.levelRegions[self.session.cLevelId]['finish'].getCenter()
        playersPosition = self.getPlayersClosest(completedAt, finishCenter['x'], finishCenter['y'])
        bcount = len(self.getActivePlayers())
        rcount = self.mapInfo.levelRegions[self.session.cLevelId]['finPlayers'](bcount)
        self.logGame('bcount=%d rcount=%d pos=%s' % (bcount, rcount, playersPosition))

        completedBy = []
        for i in range(rcount):
            playerId = playersPosition[i]['playerId']
            completedBy.append(playerId)
            self.session.playerStats[playerId]['level'] += 1

        self.session.levels[self.session.cLevelId] = {
            'created_at': self.session.clInitAt,
            'started_at': startedAt,
            'completed_at': completedAt,
            'completed_by': completedBy,
            'powerups_by': powerupsBy,
            'completed_time': secs,
            'order': len(self.session.levels)
        }
        self.logGame('[%02d/%02d] Level %d completed in %.2fs by %s' % (
            len(self.session.levels),
            len(self.mapInfo.levelRegions),
            self.session.cLevelId,
            secs,
            ', '.join(map(lambda x: self.playerMap[x]['name'], completedBy))
        ))
        self.session.clearMoveOrders()
        self.session.clPowerups = []

    def process(self):
        while True:
            try:
                ev = self.next()
                if ev['_event'].startswith('NNet.Replay.Tracker'):
                    self.unState.onEvent(ev)
                self.gameloop = ev['_gameloop']

                if ev['_gameloop'] <= 0:
                    continue

                if ev['_event'] == 'NNet.Replay.Tracker.SUnitBornEvent':
                    unit = self.unState.units[ev['m_unitTagIndex']]

                    if unit['unitTypeName'] == 'IceBaneling':
                        if self.session.gameStartedAt is None:
                            self.session.gameStartedAt = ev['_gameloop']
                            self.logGame(' === GAME STARTED === ')
                            self.logGame('timefactor=%f' % self.timeFactor)
                        self.session.banelings[unit['controlPlayerId']] = unit
                        self.session.createPlayer(unit['controlPlayerId'])
                        self.logGame('P%0d IceBaneling born' % (unit['controlPlayerId']), playerId=unit['controlPlayerId'])
                    elif unit['unitTypeName'] == 'Beacon_ZergSmall2':
                        self.session.playerStats[unit['controlPlayerId']]['deaths'] += 1
                        # self.logGame('P%0d IceBaneling died' % (unit['controlPlayerId']), playerId=unit['controlPlayerId'])
                    elif unit['unitTypeName'] == 'ShapeTorus4':
                        self.levelCompleted(ev['_gameloop'])
                        self.session.gameEscapedAt = ev['_gameloop']
                        self.logGame('GAME ESCAPED')
                        break
                    elif unit['unitTypeName'] == 'PickupChronoRiftCharge':
                        self.session.clPowerups.append(unit)

                    if (self.session.cLevelId == None or len(self.session.clUnits) == 0) and unit['controlPlayerId'] in [15]:
                        if (
                            self.mapId == 'IBE2' and
                            (
                                (
                                    unit['unitTypeName'] == 'UrsadakFemaleExotic' and
                                    self.mapInfo.levelRegions[3]['region'].containsPoint(unit['posX'], unit['posY'])
                                ) or
                                (
                                    unit['unitTypeName'] == 'Lyote' and
                                    self.mapInfo.levelRegions[26]['region'].containsPoint(unit['posX'], unit['posY'])
                                )
                            )
                        ):
                            continue
                        if self.mapId == 'IBE1' and (
                            unit['unitTypeName'] == 'RedstoneLavaCritter' and unit['posX'] == 190 and unit['posY'] == 170
                        ):
                            continue

                        if not len(self.session.clUnits):
                            if self.session.cLevelId is not None and self.mapId == 'IBE2':
                                self.levelCompleted(ev['_gameloop'])
                            self.session.cLevelId = None
                            self.session.clInitAt = ev['_gameloop']
                            if len(self.session.levels):
                                self.logGame('Level init, diff=%d' % (ev['_gameloop'] - self.session.levels.values()[-1]['completed_at']))
                            else:
                                self.logGame('Level init')

                            if self.mapId == 'IBE2':
                                if (
                                    (len(self.session.levels) in [10, 20]) or
                                    (len(self.getActivePlayers()) >= 8 and len(self.session.levels) in [5, 15])
                                ):
                                    for playerId in self.getActivePlayers():
                                        self.session.playerStats[playerId]['level'] += 1
                                    self.logGame('Extra level-up acquired: %s' % map(lambda x: self.playerMap[x]['name'], self.getActivePlayers()))

                        self.session.clUnits.append(unit)

                elif ev['_event'] == 'NNet.Replay.Tracker.SUnitDiedEvent':
                    unit = self.unState.units[ev['m_unitTagIndex']]

                    if unit['unitTypeName'] == 'IceBaneling2' and self.session.gameStartedAt is not None:
                        if len(self.unState.fetchUnits(unitName='IceBaneling2')) == 0:
                            self.session.clear()
                            self.logGame('GAME FAILED')

                    doCleanup = False
                    # === HARDCODED RULES ===
                    if self.session.cLevelId != None and len(self.session.clUnits) > 0:
                        # IBE1
                        if self.mapId == 'IBE1' and self.session.cLevelId == 20:
                            if unit['unitTypeName'] == 'RedstoneLavaCritter' and unit['posX'] == 222 and unit['posY'] == 170:
                                doCleanup = True
                        # IBE2
                        elif self.mapId == 'IBE2' and self.session.cLevelId == 26:
                            if unit['unitTypeName'] == 'RedstoneLavaCritter':
                                doCleanup = True
                    # === HARDCODED RULES ===

                    if self.session.cLevelId != None and len(self.session.clUnits) > 0:
                        # if self.mapId == 'IBE2' and self.trEvents.peek['_event'] != 'NNet.Replay.Tracker.SUnitDiedEvent':
                        #     continue
                        # if self.session.cLevelId == 0:
                        #     continue
                        # self.logGame('u %f' % (float(len(self.session.getLivingUnits())) / float(len(self.session.clUnits))))
                        if self.mapId == 'IBE2' and len(self.unState.fetchUnits(playerIds=[15], unitName="PhoenixLow")):
                            continue
                        if (float(len(self.session.getLivingUnits())) / float(len(self.session.clUnits))) < 0.5:
                            doCleanup = True

                    if doCleanup:
                        self.logGame('Level cleanup')
                        self.session.clUnits = []
                        if self.mapId == 'IBE1' and self.session.cLevelId != 0:
                            self.levelCompleted(ev['_gameloop'])

                elif ev['_event'] == 'NNet.Game.SCameraUpdateEvent' and ev['m_target'] != None:
                    posX = ev['m_target']['x'] / 256.0
                    posY = ev['m_target']['y'] / 256.0
                    # self.logGame('Camera update [ %5.1f ; %5.1f ]' % (posX, posY), userId=ev['_userid']['m_userId'])
                    if self.session.cLevelId == None and len(self.session.clUnits):
                        if self.gmEvents.peek['_event'] == 'NNet.Game.SCameraUpdateEvent':
                            if self.mapId == 'IBE1' and (self.gmEvents.peek['_gameloop'] - self.session.clInitAt) < 10:
                                continue
                            elif self.mapId == 'IBE2' and (self.gmEvents.peek['_gameloop'] - self.session.clInitAt) < 10:
                                continue
                        if self.mapId == 'IBE1' and len(self.session.levels) == 20 and self.mapInfo.findClosestLevel('spawn', posX, posY) != 0:
                            continue
                        self.session.cLevelId = self.mapInfo.findClosestLevel('spawn', posX, posY)
                        tmpCenter = self.mapInfo.levelRegions[self.session.cLevelId]['spawn'].getCenter()
                        if math.hypot(tmpCenter['x'] - posX, tmpCenter['y'] - posY) > 10.0:
                            self.session.cLevelId = None
                            continue
                        self.logGame('camera lvl %d' % self.session.cLevelId)
                        # self.logGame('%d' % len(self.unState.fetchUnits(playerIds=[15])))

                elif ev['_event'] == 'NNet.Game.SCmdUpdateTargetPointEvent':
                    posX = ev['m_target']['x'] / 4096.0
                    posY = ev['m_target']['y'] / 4096.0
                    playerId = self.userMap[ev['_userid']['m_userId']]['player_id']
                    self.session.registerMoveOrder(ev['_gameloop'], playerId, posX, posY)
                    # self.logGame('Target update [ %5.1f ; %5.1f ]' % (posX, posY), userId=ev['_userid']['m_userId'])

                elif ev['_event'] == 'NNet.Game.SCmdEvent':
                    # ev['m_cmdFlags']
                    playerId = self.userMap[ev['_userid']['m_userId']]['player_id']
                    if ev['m_abil'] is None and 'TargetPoint' in ev['m_data']:
                        posX = ev['m_data']['TargetPoint']['x'] / 4096.0
                        posY = ev['m_data']['TargetPoint']['y'] / 4096.0
                        self.session.registerMoveOrder(ev['_gameloop'], playerId, posX, posY)
                        # self.logGame('Target update [ %5.1f ; %5.1f ]' % (posX, posY), userId=ev['_userid']['m_userId'])
                    elif ev['m_abil'] is not None:
                        if ev['m_abil']['m_abilLink'] not in self.session.abilRawUsage[playerId]:
                            self.session.abilRawUsage[playerId][ev['m_abil']['m_abilLink']] = []
                        if (
                            (ev['m_cmdFlags'] & CmdFlags.Queued or ev['m_cmdFlags'] & CmdFlags.Repeat) and
                            (len(self.session.abilRawUsage[playerId][ev['m_abil']['m_abilLink']]))
                        ):
                            prevAbil = self.session.abilRawUsage[playerId][ev['m_abil']['m_abilLink']][-1]
                            if (
                                (prevAbil['flags'] & CmdFlags.Queued or prevAbil['flags'] & CmdFlags.Repeat) and
                                (ev['_gameloop'] - prevAbil['gameloop']) < 5 * 16
                            ):
                                continue
                        self.session.abilRawUsage[playerId][ev['m_abil']['m_abilLink']].append({
                            'link': ev['m_abil']['m_abilLink'],
                            'index': ev['m_abil']['m_abilCmdIndex'],
                            'flags': ev['m_cmdFlags'],
                            'gameloop': ev['_gameloop'],
                        })

                elif ev['_event'] == 'NNet.Game.SGameUserLeaveEvent':
                    playerId = self.userMap[ev['_userid']['m_userId']]['player_id']
                    self.playersLeft[playerId] = ev['_gameloop']
                    self.logGame('player left', playerId=playerId)

            except StopIteration:
                break

    def determineAbilityLinks(self, deltaResult):
        totalCount = OrderedDict()
        for playerId in self.session.abilRawUsage:
            for abilLink in self.session.abilRawUsage[playerId]:
                if abilLink not in totalCount:
                    totalCount[abilLink] = 0
                totalCount[abilLink] += len(self.session.abilRawUsage[playerId][abilLink])
                # self.logGame('p%d link %d = %d' % (playerId, abilLink, len(self.session.abilRawUsage[playerId][abilLink])))

        abilMap = [
            {
                'deltaName': 'used_propel_times',
                'abilId': 1,
            },
            {
                'deltaName': 'used_power_boost_times',
                'abilId': 0,
            },
            {
                'deltaName': 'used_throw_essence_times',
                'abilId': 2,
            },
            {
                'deltaName': 'used_rev_art_times',
                'abilId': 3,
            },
        ]
        if self.mapId == 'IBE2':
            abilMap.append({
                'deltaName': 'used_time_shift_times',
                'abilId': 8,
            })

        matchedLinks = OrderedDict()
        for abilInfo in abilMap:
            abilData = []
            for abilLink in totalCount:
                if abilLink in matchedLinks:
                    continue
                abilData.append({
                    'abilId': abilInfo['abilId'],
                    'abilLink': abilLink,
                    'totalCount': totalCount[abilLink],
                    'score': abs(totalCount[abilLink] - deltaResult['team'][abilInfo['deltaName']]),
                })
            abilData.sort(key=lambda x: x['score'])
            matchedLinks[abilData[0]['abilLink']] = abilData[0]

        return matchedLinks

    def rebuildGameResult(self, deltaResult):
        ABIL_NAMES = [
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

        result = OrderedDict()
        result['game_diff'] = deltaResult['game_diff']
        result['game_speed'] = deltaResult['game_speed']
        result['framework_version'] = None
        result['game_version'] = deltaResult['minor_version']
        result['escape_time'] = deltaResult['escape_time']
        result['escaped'] = deltaResult['escaped']

        result['started_at_rt'] = None
        result['started_at_gt'] = round(self.session.gameStartedAt / 16.0, 2)

        result['challenges_completed'] = len(self.session.levels)
        result['challenges_total'] = len(self.mapInfo.levelRegions)

        result['players'] = OrderedDict()
        for playerId in self.playerMap:
            result['players'][playerId] = OrderedDict()
            # result['players'][playerId]['left'] = deltaResult['players'][playerId]['left']
            result['players'][playerId]['left'] = True if playerId in self.playersLeft else False
            if playerId in self.session.playerStats:
                result['players'][playerId]['level'] = self.session.playerStats[playerId]['level']
                result['players'][playerId]['deaths'] = self.session.playerStats[playerId]['deaths']
                result['players'][playerId]['revives'] = None
                # TODO: revives

                result['players'][playerId]['abilities_used'] = OrderedDict()
                for i, name in enumerate(ABIL_NAMES):
                    result['players'][playerId]['abilities_used'][ABIL_NAMES[i]] = None
                matchedAbils = self.determineAbilityLinks(deltaResult)
                for abilLink in matchedAbils:
                    abilName = ABIL_NAMES[matchedAbils[abilLink]['abilId']]
                    try:
                        result['players'][playerId]['abilities_used'][abilName] = len(self.session.abilRawUsage[playerId][abilLink])
                    except KeyError:
                        result['players'][playerId]['abilities_used'][abilName] = 0
            else:
                result['players'][playerId]['level'] = None
                result['players'][playerId]['deaths'] = None

        result['challenges'] = OrderedDict()
        for chalId in self.session.levels:
            result['challenges'][chalId] = OrderedDict()
            result['challenges'][chalId]['completed_by'] = self.session.levels[chalId]['completed_by']
            if len(result['challenges'][chalId]['completed_by']) <= 0:
                result['challenges'][chalId]['completed_by'] = [15]
            result['challenges'][chalId]['completed_by'] = map(lambda x: [x, x], result['challenges'][chalId]['completed_by'])
            result['challenges'][chalId]['buttons_by'] = []
            # TODO: buttons_by
            result['challenges'][chalId]['powerups_by'] = self.session.levels[chalId]['powerups_by']
            result['challenges'][chalId]['powerups_by'] = map(lambda x: [x, x], result['challenges'][chalId]['powerups_by'])
            result['challenges'][chalId]['completed_time'] = round(self.session.levels[chalId]['completed_time'], 2)
            result['challenges'][chalId]['time_offset_start'] = round(
                (self.session.levels[chalId]['started_at'] - self.session.gameStartedAt) / (16.0 * self.timeFactor),
                2
            )
            result['challenges'][chalId]['order'] = self.session.levels[chalId]['order']

        if len(self.session.levels) != len(self.mapInfo.levelRegions):
            raise Exception('Levels completed count doesn\'t match with total count')

        return result

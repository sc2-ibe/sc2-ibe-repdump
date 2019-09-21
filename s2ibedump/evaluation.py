#!/usr/bin/python2
# -*- coding: utf-8 -*-

from __future__ import print_function
from pprint import pprint, pformat
import sys
import copy
import logging
import math
from collections import OrderedDict
from .s2map import MapInfo, CmdFlags
from .helpers import unitTagIndex, toJson


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

    def fetchUnits(self, playerIds=None, unitName=None, posX=None, posY=None, createdAt=None, includeRemoved=False):
        r = []
        for key in self.units:
            if self.units[key]['removed'] >= 0 and not includeRemoved:
                continue
            if createdAt is not None and self.units[key]['createdAt'] != createdAt:
                continue
            if playerIds != None and self.units[key]['controlPlayerId'] not in playerIds:
                continue
            if unitName != None:
                if isinstance(unitName, str) and self.units[key]['unitTypeName'] != unitName:
                    continue
                elif isinstance(unitName, list) and self.units[key]['unitTypeName'] not in unitName:
                    continue
            if posX != None and self.units[key]['posX'] != posX:
                continue
            if posY != None and self.units[key]['posY'] != posY:
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
        self.gameSpeed = None
        self.gameDiff = 1
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
        self.cameraUpdates = OrderedDict()
        self.playerSelection = OrderedDict()
        self.ctrlGroups = OrderedDict()

    def clearMoveOrders(self):
        for i in range(10):
            self.moveOrders[i + 1] = []

    def clearCameraUpdates(self):
        for i in range(10):
            self.cameraUpdates[i + 1] = []

    def createPlayer(self, playerId):
        self.moveOrders[playerId] = []
        self.cameraUpdates[playerId] = []
        self.playerStats[playerId] = {
            'deaths': 0,
            'level': 1,
        }
        self.abilRawUsage[playerId] = OrderedDict()
        self.playerSelection[playerId] = []
        self.ctrlGroups[playerId] = [list() for x in range(10)]

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
        tmpPlayerSelection = self.playerSelection[playerId]
        if len(tmpPlayerSelection) == 0:
            tmpPlayerSelection = [playerId]
        for selectedPlayerId in tmpPlayerSelection:
            self.moveOrders[selectedPlayerId].append({
                'gameloop': gameloop,
                'x': posX,
                'y': posY,
                'ctrlPlayerId': playerId,
            })

    def registerCameraUpdate(self, gameloop, playerId, posX, posY, yaw, pitch):
        try:
            self.cameraUpdates[playerId].append({
                'gameloop': gameloop,
                'x': posX,
                'y': posY,
                'yaw': yaw,
                'pitch': pitch,
            })
        except KeyError:
            pass

    def findInitialCamPosition(self, fetchLatest=False, resolutionDiv=4.0):
        tPosMap = OrderedDict()
        for playerId in self.cameraUpdates:
            for item in self.cameraUpdates[playerId]:
                poskey = '%03d;%03d' % (int(round(item['x'] / resolutionDiv)), int(round(item['y'] / resolutionDiv)))
                if poskey not in tPosMap:
                    tPosMap[poskey] = {
                        'playerIds': {},
                        'x': item['x'],
                        'y': item['y'],
                    }
                if playerId not in tPosMap[poskey]['playerIds']:
                    tPosMap[poskey]['playerIds'][playerId] = item['gameloop']

        bestPick = None
        bestCounter = None
        for poskey in tPosMap:
            if (
                (bestPick is None) or
                (len(tPosMap[poskey]['playerIds']) > bestCounter) or
                (fetchLatest == True and len(tPosMap[poskey]['playerIds']) >= bestCounter)
            ):
                bestPick = poskey
                bestCounter = len(tPosMap[poskey]['playerIds'])

        # pprint(tPosMap, stream=sys.stderr)

        return tPosMap[bestPick] if bestPick is not None else None

    def getLatestCameraPos(self, playerId=None):
        latestPositions = {}
        for currPlayerId in self.cameraUpdates:
            if playerId is not None and currPlayerId != playerId:
                continue
            for item in self.cameraUpdates[currPlayerId]:
                if currPlayerId not in latestPositions:
                    latestPositions[currPlayerId] = {
                        'playerId': currPlayerId,
                        'yaw': 90.0,
                        'pitch': 60.0,
                    }
                latestPositions[currPlayerId]['gameloop'] = item['gameloop']
                if item['x']:
                    latestPositions[currPlayerId]['x'] = item['x']
                if item['y']:
                    latestPositions[currPlayerId]['y'] = item['y']
                if item['yaw']:
                    latestPositions[currPlayerId]['yaw'] = item['yaw']
                if item['pitch']:
                    latestPositions[currPlayerId]['pitch'] = item['pitch']

        if playerId is not None:
            return latestPositions[playerId]

        items = latestPositions.values()
        items.sort(key=lambda x: x['gameloop'], reverse=True)
        return items[0]

    def estimatePlayerPosition(self, playerId, atGameloop, startPos):
        currPos = {
            'x': startPos['x'],
            'y': startPos['y'],
        }
        currGameloop = None

        for key, morder in enumerate(self.moveOrders[playerId]):
            if morder['gameloop'] > atGameloop:
                break

            morder = copy.deepcopy(morder)
            try:
                # in case of orders issued in the same gameloop, skip to latest one
                if self.moveOrders[playerId][key + 1]['gameloop'] == morder['gameloop']:
                    continue
                elif self.moveOrders[playerId][key + 1]['gameloop'] > atGameloop:
                    morder['gameloop'] = atGameloop
            except IndexError:
                pass

            if currGameloop is None:
                currPos['x'] = morder['x']
                currPos['y'] = morder['y']
            else:
                distance = math.hypot(morder['x'] - currPos['x'], morder['y'] - currPos['y'])
                distLoops = float(morder['gameloop'] - currGameloop)
                if distance > 2.0 and distLoops < (16.0 * 2.5):
                    distX = morder['x'] - currPos['x']
                    distY = morder['y'] - currPos['y']
                    currPos['x'] += min(distX, (distX / distance) * 0.375 * distLoops)
                    currPos['y'] += min(distY, (distY / distance) * 0.375 * distLoops)
                else:
                    currPos['x'] = morder['x']
                    currPos['y'] = morder['y']

            currGameloop = morder['gameloop']

        return currPos

    def getPlayerCtrl(self, playerId, atGameloop):
        ctrlPlayerId = playerId
        for morder in self.moveOrders[playerId]:
            if morder['gameloop'] > atGameloop:
                break
            ctrlPlayerId = morder['ctrlPlayerId']
        return ctrlPlayerId


class GameEvaluation(object):
    def __init__(self, baseBuild, mapId, playerSlots, trEvents, gmEvents, defaultGameSpeed):
        self.baseBuild = baseBuild
        self.mapId = mapId
        self.playerSlots = playerSlots
        self.trEvents = EventStream(trEvents)
        self.gmEvents = EventStream(gmEvents)
        self.timeFactor = None
        self.defaultGameSpeed = defaultGameSpeed
        self.gameloop = 0
        self.unState = UnitState()
        self.mapInfo = MapInfo(mapId)
        self.session = GameSession()
        self.playersLeft = {}
        self.userMap = {}
        self.playerMap = {}
        self.hijackReplayGameEvent = None
        for x in self.playerSlots:
            if isinstance(x['user_id'], int):
                self.userMap[x['user_id']] = x
            if x['player_id'] is not None:
                self.playerMap[x['player_id']] = x

    def next(self):
        trEv = self.trEvents.peek
        gmEv = self.gmEvents.peek
        if trEv and (not gmEv or trEv['_gameloop'] <= gmEv['_gameloop']):
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
            out += ' P%02d <%s>' % (playerId, self.playerMap[playerId]['name'])

        out += ' %s' % msg

        logging.debug(out)

    def playerFromUser(self, userId):
        return self.userMap[userId]

    def isPlayerAlive(self, playerId, gameloop):
        beacons = self.unState.fetchUnits(playerIds=[playerId], unitName='Beacon_ZergSmall2', includeRemoved=True)
        validBeacons = []
        for item in beacons:
            if item['removed'] != -1 and item['removed'] <= gameloop:
                continue
            if item['createdAt'] <= self.session.clInitAt:
                continue
            if item['createdAt'] <= gameloop:
                validBeacons.append(item)
        return len(validBeacons) == 0

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
            # if not self.isPlayerAlive(playerId, gameloop):
            #     continue
            position = self.session.estimatePlayerPosition(playerId, gameloop, self.mapInfo.levelRegions[self.session.cLevelId]['spawn'].getCenter())
            distance = math.hypot(
                position['x'] - targetX,
                position['y'] - targetY
            )
            playersPosition.append({
                'playerId': playerId,
                'ctrlPlayerId': self.session.getPlayerCtrl(playerId, gameloop),
                'distance': distance,
                'alive': self.isPlayerAlive(playerId, gameloop),
                'onSpawn': len(self.session.moveOrders[playerId]) == 0
            })

        def closest(item):
            d = item['distance']
            if not item['alive'] or item['onSpawn']:
                d += 40.0
            return d

        playersPosition.sort(key=closest)

        return playersPosition

    def fetchMatchingLevelRegion(self):
        obstacleLevelRegions = []

        for chalId in self.mapInfo.levelRegions:
            tmpLevel = {
                'chalId': chalId,
                'obstacles': [],
            }
            for obstacle in self.session.clUnits:
                if self.mapInfo.levelRegions[chalId]['region'].containsPoint(obstacle['posX'], obstacle['posY']):
                    tmpLevel['obstacles'].append(obstacle)
            obstacleLevelRegions.append(tmpLevel)

        obstacleLevelRegions.sort(key=lambda x: len(x['obstacles']), reverse=True)
        return obstacleLevelRegions

    def getLevelStartedAt(self):
        startedAt = self.session.clInitAt
        if self.mapId in ['IBE1', 'RIBE1']:
            startedAt += 16.0 * 1.0 * self.timeFactor
            if self.session.cLevelId == self.mapInfo.finalLevel:
                startedAt += 16.0 * 8.0
        return startedAt

    def levelCompleted(self, gameloopEnd):
        if self.session.cLevelId == 0:
            completedAt = gameloopEnd
        else:
            completedAt = gameloopEnd
            if self.mapId == 'IBE2':
                completedAt -= (16.0 * 3.0 * self.timeFactor)

            if self.mapId in ['IBE1', 'IBE2', 'RIBE1']:
                completedAt -= (16.0 * 1.5 * self.timeFactor)
            elif self.mapId.startswith('IBE-CV'):
                if self.session.cLevelId != self.mapInfo.finalLevel:
                    completedAt -= math.ceil(16.0 * 2.0 * self.timeFactor)
                    completedAt -= math.ceil(16.0 * 1.5 * self.timeFactor)
                else:
                    completedAt -= math.ceil(16.0 * 1.5)
                    completedAt -= math.ceil(16.0 * 0.2)
                completedAt -= 1

        startedAt = self.getLevelStartedAt()

        ticks = completedAt - startedAt
        secs = ticks / (16.0 * self.timeFactor)

        finishCenter = self.mapInfo.levelRegions[self.session.cLevelId]['finish'].getCenter()
        self.logGame('finish region [ %5.1f ; %5.1f ]' % (finishCenter['x'], finishCenter['y']))
        playersPosition = self.getPlayersClosest(completedAt, finishCenter['x'], finishCenter['y'])
        bcount = len(self.session.banelings.values())
        rcount = self.mapInfo.levelRegions[self.session.cLevelId]['finPlayers'](bcount)
        self.logGame('bcount=%d rcount=%d pos=%s' % (bcount, rcount, playersPosition), gameloop=completedAt)

        if len(playersPosition) == 0:
            self.logGame('Level failed')
            self.levelDone(gameloopEnd)
            return

        if self.mapId.startswith('IBE-CV') and self.session.cLevelId in [28] and playersPosition[0]['distance'] > 5.0:
            self.logGame('Level failed')
            self.levelDone(gameloopEnd)
            return

        if self.session.cLevelId in self.session.levels:
            raise Exception('Level %d already completed - missmatch?' % self.session.cLevelId)

        completedBy = []
        for i in range(rcount):
            completedBy.append([
                playersPosition[i]['playerId'],
                playersPosition[i]['ctrlPlayerId']
            ])
            self.session.playerStats[playersPosition[i]['playerId']]['level'] += 1

        powerupsBy = []
        for item in self.session.clPowerups:
            if item['removed'] == -1 or item['removed'] >= gameloopEnd:
                continue
            playersPosition = self.getPlayersClosest(item['removed'], item['posX'], item['posY'])
            powerupsBy.append([
                playersPosition[0]['playerId'],
                playersPosition[0]['ctrlPlayerId']
            ])
            self.session.playerStats[playersPosition[0]['playerId']]['level'] += 1
            self.logGame(
                'Powerup acquired - removedAt=%d %s' % (item['removed'], playersPosition[0]),
                gameloop=item['removed'],
                playerId=playersPosition[0]['ctrlPlayerId']
            )

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
            ', '.join(map(lambda x: '%s [%s]' % (self.playerMap[x[0]]['name'], self.playerMap[x[1]]['name']), completedBy))
        ), gameloop=completedAt)
        self.levelDone(gameloopEnd)

    def levelFailed(self):
        self.session.levels[self.session.cLevelId] = {
            'created_at': self.session.clInitAt,
            'started_at': self.getLevelStartedAt(),
            'completed_at': self.gameloop,
            'completed_by': None,
            'powerups_by': None,
            'completed_time': None,
            'order': len(self.session.levels)
        }
        self.logGame('[%02d/%02d] Level %d failed' % (
            len(self.session.levels),
            len(self.mapInfo.levelRegions),
            self.session.cLevelId
        ))

    def levelDone(self, gameLoop):
        self.session.clearMoveOrders()
        self.session.clearCameraUpdates()
        self.session.clPowerups = filter(lambda x: x['createdAt'] == gameLoop, self.session.clPowerups)

    def process(self):
        while True:
            try:
                ev = self.next()
                if ev['_event'].startswith('NNet.Replay.Tracker'):
                    # Only 4 point resolution prior to 2.1
                    if self.baseBuild < 27950 and 'm_x' in ev:
                        ev['m_x'] = ev['m_x'] * 4
                        ev['m_y'] = ev['m_y'] * 4
                    self.unState.onEvent(ev)
                self.gameloop = ev['_gameloop']

                if ev['_event'] == 'NNet.Replay.Tracker.SPlayerSetupEvent':
                    # self.logGame(pformat(ev))
                    pass
                elif ev['_event'] == 'NNet.Game.SGameUserLeaveEvent':
                    userId = ev['_userid']['m_userId']
                    if userId >= 10:
                        self.logGame('userid 10, wtf?')
                        continue
                    playerId = self.playerFromUser(userId)['player_id']
                    if playerId:
                        self.playersLeft[playerId] = ev['_gameloop']
                        self.logGame('player left', playerId=playerId)
                    else:
                        self.logGame('user left', userId=userId)

                elif ev['_event'] == 'NNet.Game.SHijackReplayGameEvent':
                    self.hijackReplayGameEvent = ev['_gameloop']

                elif ev['_event'] == 'NNet.Game.SGameUserJoinEvent':
                    self.logGame('unhandled SGameUserJoinEvent')
                    self.logGame(pformat(ev))
                    break

                if ev['_gameloop'] <= 0 and self.mapId != 'IBE1':
                    continue

                if ev['_event'] == 'NNet.Replay.Tracker.SUnitBornEvent':
                    unit = self.unState.units[ev['m_unitTagIndex']]

                    if unit['unitTypeName'] == 'IceBaneling' and unit['controlPlayerId'] != 0:
                        if self.session.gameStartedAt is None:
                            self.session.gameStartedAt = ev['_gameloop']
                            self.logGame(' === GAME STARTED === %s' % self.mapId)
                        self.session.banelings[unit['controlPlayerId']] = unit
                        self.session.createPlayer(unit['controlPlayerId'])
                        self.logGame('IceBaneling born [ %5.1f ; %5.1f ]' % (unit['posX'], unit['posY']), playerId=unit['controlPlayerId'])

                        if self.mapId in ['IBE1', 'IBE2', 'RIBE1']:
                            peekEv = self.trEvents.peek
                            if peekEv['_event'] != 'NNet.Replay.Tracker.SUnitBornEvent' or peekEv['m_unitTypeName'] != 'IceBaneling':
                                pass
                    elif unit['unitTypeName'] == 'Beacon_ZergSmall2' and unit['controlPlayerId'] != 0:
                        self.session.playerStats[unit['controlPlayerId']]['deaths'] += 1
                        self.logGame('IceBaneling died [ %5.1f ; %5.1f ]' % (unit['posX'], unit['posY']), playerId=unit['controlPlayerId'])
                        for ctrlPlayerId in self.session.playerSelection:
                            try:
                                self.session.playerSelection[ctrlPlayerId].remove(unit['controlPlayerId'])
                            except ValueError:
                                pass
                    elif unit['unitTypeName'] == 'ShapeTorus4':
                        self.levelCompleted(ev['_gameloop'])
                        self.session.gameEscapedAt = ev['_gameloop']
                        self.logGame('GAME ESCAPED')
                        yield self.session
                    elif unit['unitTypeName'] == 'PickupChronoRiftCharge':
                        self.session.clPowerups.append(unit)

                    if self.mapId in ['IBE-CV', 'IBE-CV-PRO'] and (
                        unit['unitTypeName'] == 'sfBushLarge' and unit['posX'] == 193 and unit['posY'] == 225
                    ):
                        self.session.gameDiff = 2
                        self.logGame('Extreme mode detected')

                    if (self.session.cLevelId == None or len(self.session.clUnits) == 0) and unit['controlPlayerId'] in [self.mapInfo.obstaclePlayerId]:
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
                        if self.mapId in ['IBE1', 'RIBE1'] and (
                            unit['unitTypeName'] == 'RedstoneLavaCritter' and unit['posX'] == 190 and unit['posY'] == 170
                        ):
                            continue

                        if not len(self.session.clUnits) and self.session.gameStartedAt is not None:
                            if self.session.cLevelId is not None and self.mapId == 'IBE2':
                                self.levelCompleted(ev['_gameloop'])
                            self.session.cLevelId = None
                            self.session.clInitAt = ev['_gameloop']
                            if len(self.session.levels):
                                tdiff = ev['_gameloop'] - self.session.levels.values()[-1]['completed_at']
                                self.logGame('Level init, tdiff=%d' % (tdiff))
                                if self.mapId == 'IBE1' and len(self.session.levels) == 1 and tdiff < 90:
                                    self.session.gameSpeed = 2
                                    self.timeFactor = 1.0
                                    self.logGame('Speed changed to normal, timefactor=%f' % self.timeFactor)
                                    # TODO: recalc first challenge
                            else:
                                if not self.timeFactor:
                                    if self.mapId.startswith('IBE-CV'):
                                        if (ev['_gameloop'] - self.session.gameStartedAt) == 16:
                                            self.session.gameSpeed = 2
                                        else:
                                            self.session.gameSpeed = 3
                                    elif self.mapId in ['IBE1', 'IBE2', 'RIBE1']:
                                        self.session.gameSpeed = self.defaultGameSpeed

                                    if self.session.gameSpeed == 2:
                                        self.timeFactor = 1.0
                                        # 256 normal
                                    elif self.session.gameSpeed == 3:
                                        self.timeFactor = 1.201935
                                        # 256/213 = 1.20187793427
                                        # 213 fast?
                                    elif self.session.gameSpeed == 4:
                                        self.timeFactor = 1.4
                                        # 182 faster
                                    else:
                                        raise Exception()
                                    self.logGame('timefactor=%f' % self.timeFactor)
                                self.logGame('Level init')

                            if self.mapId.startswith('IBE-CV'):
                                initCam = self.session.findInitialCamPosition(fetchLatest=self.mapId.startswith('IBE-CV'), resolutionDiv=4.0)
                                if not initCam:
                                    if len(self.session.levels) == 0:
                                        baneling = self.session.banelings.values()[0]
                                        initCam = {
                                            'x': baneling['posX'],
                                            'y': baneling['posY'],
                                        }
                                    else:
                                        raise Exception('find initcam failed')
                                # self.logGame(pformat(initCam))
                                self.session.cLevelId = self.mapInfo.findClosestLevel('spawn', initCam['x'], initCam['y'])
                                self.logGame('camera lvl %d' % self.session.cLevelId)

                            extraLevelup = False
                            if self.mapId == 'IBE2':
                                if (
                                    (len(self.session.levels) in [10, 20]) or
                                    (len(self.session.banelings.values()) >= 8 and len(self.session.levels) in [5, 15])
                                ):
                                    extraLevelup = True
                            elif (
                                self.mapId.startswith('IBE-CV') and
                                len(self.session.levels) > 0 and len(self.session.levels) % 5 == 0
                            ):
                                extraLevelup = True
                            if extraLevelup:
                                for playerId in self.session.banelings:
                                    self.session.playerStats[playerId]['level'] += 1
                                self.logGame('Extra level-up acquired: %s' % map(lambda x: self.playerMap[x]['name'], self.session.banelings.keys()))

                        if self.session.gameStartedAt is not None:
                            self.session.clUnits.append(unit)
                    elif self.session.cLevelId is not None and self.mapId.startswith('IBE-CV'):
                        if ev['_gameloop'] == self.session.clInitAt and unit['controlPlayerId'] in [self.mapInfo.obstaclePlayerId]:
                            self.session.clUnits.append(unit)

                elif ev['_event'] == 'NNet.Replay.Tracker.SUnitDiedEvent':
                    unit = self.unState.units[ev['m_unitTagIndex']]

                    if unit['unitTypeName'] in ['IceBaneling2', 'IceBaneCollnDetec'] and self.session.gameStartedAt is not None:
                        # old IBE1 (early ~2013) used `IceBaneCollnDetec` instead of `IceBaneling2`
                        if len(self.unState.fetchUnits(unitName=['IceBaneling2', 'IceBaneCollnDetec'])) == 0:
                            self.levelFailed()
                            self.logGame('GAME FAILED')
                            yield self.session
                            self.session.clear()
                            self.timeFactor = None

                    doCleanup = False
                    # === HARDCODED RULES ===
                    if self.session.cLevelId != None and len(self.session.clUnits) > 0:
                        currLevelOverride = None

                        # IBE1
                        if self.mapId in ['IBE1', 'RIBE1']:
                            if self.session.cLevelId == 20:
                                obstCount = len(self.unState.fetchUnits(
                                    unitName="RedstoneLavaCritter",
                                    createdAt=self.session.clInitAt,
                                ))
                                if not obstCount:
                                    doCleanup = True
                                else:
                                    continue

                            if self.session.cLevelId != 9 and currLevelOverride is None:
                                obstCount = len(self.unState.fetchUnits(
                                    unitName="PrisonZealot",
                                    posX=36,
                                    posY=30,
                                    createdAt=self.session.clInitAt,
                                    includeRemoved=True
                                ))
                                if obstCount:
                                    currLevelOverride = 9
                                    doCleanup = True
                        # IBE2
                        elif self.mapId == 'IBE2' and self.session.cLevelId == 26:
                            if unit['unitTypeName'] == 'RedstoneLavaCritter':
                                doCleanup = True
                            else:
                                continue
                        # IBE-CV IBE-CV-PRO
                        elif self.mapId in ['IBE-CV', 'IBE-CV-PRO']:
                            if self.session.cLevelId != 2 and currLevelOverride is None:
                                obstCount = len(self.unState.fetchUnits(
                                    unitName="HammerSecurity",
                                    posX=41,
                                    posY=58,
                                    createdAt=self.session.clInitAt,
                                    includeRemoved=True)
                                )
                                if obstCount:
                                    currLevelOverride = 2
                            if self.session.cLevelId != 7 and currLevelOverride is None:
                                obstCount = len(self.unState.fetchUnits(
                                    unitName="Ravager",
                                    posX=158,
                                    posY=47,
                                    createdAt=self.session.clInitAt,
                                    includeRemoved=True)
                                )
                                if obstCount:
                                    currLevelOverride = 7

                        if currLevelOverride is not None:
                            self.session.cLevelId = currLevelOverride
                            self.logGame('Level override %d' % (self.session.cLevelId))
                    # === HARDCODED RULES ===

                    if self.session.cLevelId != None and len(self.session.clUnits) > 0:
                        # self.logGame('u %f' % (float(len(self.session.getLivingUnits())) / float(len(self.session.clUnits))))
                        if self.mapId == 'IBE2' and len(self.unState.fetchUnits(playerIds=[15], unitName="PhoenixLow")):
                            continue
                        if self.mapId in ['IBE1', 'IBE2', 'RIBE1']:
                            # ignore obstacles removed at final level
                            # for instance when zealots are removed after pressing button in IBE1
                            if self.session.cLevelId == self.mapInfo.finalLevel:
                                if self.mapId == 'IBE1' and len(self.session.getLivingUnits()) == 0:
                                    doCleanup = True
                            elif (float(len(self.session.getLivingUnits())) / float(len(self.session.clUnits))) < 0.5:
                                doCleanup = True
                        elif self.mapId.startswith('IBE-CV'):
                            if (float(len(self.session.getLivingUnits())) / float(len(self.session.clUnits))) < 0.5:
                                doCleanup = True
                            # self.logGame(self.session.getLivingUnits())
                            # if len(self.session.getLivingUnits()) == 0:
                            #     doCleanup = True

                    if doCleanup:
                        # self.logGame(toJson(self.session.clUnits))
                        # find matching region containing alive creatures instead of relaying on user camera update
                        if len(self.session.clUnits):
                            matchingRegion = self.fetchMatchingLevelRegion()[0]
                            # self.logGame(pformat(matchingRegion))
                            if (
                                (self.mapId == 'IBE1' and len(matchingRegion['obstacles']) > 8 and matchingRegion['chalId'] not in [9]) or
                                (self.mapId == 'IBE-CV-EZ' and len(matchingRegion['obstacles']) > 5) or
                                (self.mapId in ['IBE-CV', 'IBE-CV-PRO'] and len(matchingRegion['obstacles']) > 10 and matchingRegion['chalId'] not in [2])
                            ):
                                self.session.cLevelId = matchingRegion['chalId']
                                self.logGame('Level region match %d' % (self.session.cLevelId))

                        self.logGame('Level cleanup')
                        self.session.clUnits = []
                        if self.mapId in ['IBE1', 'RIBE1'] and self.session.cLevelId != 0:
                            self.levelCompleted(ev['_gameloop'])
                        elif self.mapId.startswith('IBE-CV'):
                            if self.session.cLevelId == self.mapInfo.finalLevel:
                                self.logGame(self.session.getLatestCameraPos())
                                escaped = self.session.getLatestCameraPos()['pitch'] != 60.0 and self.session.getLatestCameraPos()['yaw'] != 90.0
                                if escaped:
                                    self.levelCompleted(ev['_gameloop'])
                                    self.session.gameEscapedAt = ev['_gameloop']
                                    self.session.gameEscapedAt -= math.ceil(16.0 * 1.5)
                                    self.session.gameEscapedAt -= math.ceil(16.0 * 0.2)
                                    self.session.gameEscapedAt -= 1
                                    self.logGame('GAME ESCAPED')
                                    yield self.session
                            else:
                                self.levelCompleted(ev['_gameloop'])
                        elif self.mapId == 'IBE1' and self.session.cLevelId == self.mapInfo.finalLevel and self.baseBuild < 26825:
                            self.levelCompleted(ev['_gameloop'])
                            self.session.gameEscapedAt = ev['_gameloop']
                            self.logGame('GAME ESCAPED (no delta result)')
                            yield self.session

                elif ev['_event'] == 'NNet.Game.SCameraUpdateEvent' and ev['m_target'] != None:
                    userId = ev['_userid']['m_userId']
                    if userId >= 10:
                        self.logGame('userid 10, wtf?')
                        continue
                    posX = ev['m_target']['x'] / 256.0
                    posY = ev['m_target']['y'] / 256.0
                    yaw = ev['m_yaw'] / 2048.0 * 360.0 if ev['m_yaw'] else None
                    pitch = ev['m_pitch'] / 2048.0 * 360.0 if ev['m_pitch'] else None
                    playerInfo = self.playerFromUser(userId)
                    if playerInfo['is_observer']:
                        continue
                    playerId = playerInfo['player_id']
                    if self.session.gameStartedAt is not None:
                        self.session.registerCameraUpdate(ev['_gameloop'], playerId, posX, posY, yaw, pitch)
                        # self.logGame('Camera update [ %5.1f ; %5.1f ]' % (posX, posY), playerId=playerId)
                    if self.session.cLevelId == None and len(self.session.clUnits):
                        if self.gmEvents.peek['_event'] == 'NNet.Game.SCameraUpdateEvent':
                            if (self.gmEvents.peek['_gameloop'] - self.session.clInitAt) < 10:
                                continue
                        if self.mapId in ['IBE1', 'RIBE1'] and len(self.session.levels) == 20 and self.mapInfo.findClosestLevel('spawn', posX, posY) != 0:
                            continue
                        # self.logGame(toJson(self.session.clUnits))

                        initCam = self.session.findInitialCamPosition()
                        if initCam is None:
                            continue
                        # self.logGame(pformat(initCam))
                        self.session.cLevelId = self.mapInfo.findClosestLevel('spawn', initCam['x'], initCam['y'])
                        tmpCenter = self.mapInfo.levelRegions[self.session.cLevelId]['spawn'].getCenter()
                        if math.hypot(tmpCenter['x'] - posX, tmpCenter['y'] - posY) > 10.0:
                            self.session.cLevelId = None
                            continue
                        self.logGame('camera lvl %d' % self.session.cLevelId)
                        # self.logGame(pformat(self.unState.fetchUnits(playerIds=[15])))
                        # self.logGame(pformat(self.session.getLivingUnits()))
                        # self.logGame('%d' % len(self.unState.fetchUnits(playerIds=[15])))
                    if self.mapId == 'IBE1' and self.session.cLevelId == self.mapInfo.finalLevel and self.baseBuild < 26825:
                        distance = math.hypot(posX - 227.0187, posY - 225.9072)
                        if distance < 2.5:
                            self.levelCompleted(ev['_gameloop'])
                            self.session.gameEscapedAt = ev['_gameloop']
                            self.logGame('GAME ESCAPED - cam fallback (no delta result)')
                            yield self.session

                elif ev['_event'] == 'NNet.Game.SCmdUpdateTargetPointEvent':
                    posX = ev['m_target']['x'] / 4096.0
                    posY = ev['m_target']['y'] / 4096.0
                    playerId = self.userMap[ev['_userid']['m_userId']]['player_id']
                    if self.session.gameStartedAt is not None:
                        self.session.registerMoveOrder(ev['_gameloop'], playerId, posX, posY)
                    # self.logGame('Target update [ %5.1f ; %5.1f ]' % (posX, posY), userId=ev['_userid']['m_userId'])

                elif ev['_event'] == 'NNet.Game.SCmdEvent':
                    # ev['m_cmdFlags']
                    playerId = self.userMap[ev['_userid']['m_userId']]['player_id']
                    if ev['m_abil'] is None and 'TargetPoint' in ev['m_data']:
                        posX = ev['m_data']['TargetPoint']['x'] / 4096.0
                        posY = ev['m_data']['TargetPoint']['y'] / 4096.0
                        if self.session.gameStartedAt is not None:
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

                elif ev['_event'] == 'NNet.Game.SSelectionDeltaEvent':
                    playerId = self.playerFromUser(ev['_userid']['m_userId'])['player_id']
                    unitTags = ev['m_delta']['m_addUnitTags']
                    selectedPlayers = []
                    for utag in unitTags:
                        unit = self.unState.units[unitTagIndex(utag)]
                        if unit['controlPlayerId'] in [0, 15]:
                            continue
                        selectedPlayers.append(unit['controlPlayerId'])
                    self.session.playerSelection[playerId] = selectedPlayers
                    if len(selectedPlayers) == 0:
                        self.logGame('Selection cleared', playerId=playerId)
                    else:
                        self.logGame('Selection updated %s' % str(map(lambda x: 'P%02d %s' % (x, self.playerMap[x]['name']), selectedPlayers)), playerId=playerId)

                elif ev['_event'] == 'NNet.Game.SControlGroupUpdateEvent':
                    playerId = self.playerFromUser(ev['_userid']['m_userId'])['player_id']
                    groupIndex = ev['m_controlGroupIndex']
                    selectedPlayers = self.session.playerSelection[playerId]
                    if ev['m_controlGroupUpdate'] == 0: # set
                        self.session.ctrlGroups[playerId][groupIndex] = copy.copy(selectedPlayers)
                        self.logGame(
                            'CtrlGroup [%d] SET %s' % (groupIndex, str(map(lambda x: 'P%02d %s' % (x, self.playerMap[x]['name']), selectedPlayers))),
                            playerId=playerId
                        )
                    elif ev['m_controlGroupUpdate'] == 1: # add
                        self.session.ctrlGroups[playerId][groupIndex] += copy.copy(selectedPlayers)
                        self.logGame(
                            'CtrlGroup [%d] ADD %s' % (groupIndex, str(map(lambda x: 'P%02d %s' % (x, self.playerMap[x]['name']), selectedPlayers))),
                            playerId=playerId
                        )
                    elif ev['m_controlGroupUpdate'] == 2: # get/use
                        self.session.playerSelection[playerId] = copy.copy(self.session.ctrlGroups[playerId][groupIndex])
                        selectedPlayers = self.session.playerSelection[playerId]
                        self.logGame(
                            'CtrlGroup [%d] USE %s' % (groupIndex, str(map(lambda x: 'P%02d %s' % (x, self.playerMap[x]['name']), selectedPlayers))),
                            playerId=playerId
                        )

            except StopIteration:
                self.logGame('end of stream')
                if self.session.cLevelId is not None:
                    self.levelFailed()
                    if self.session.cLevelId == self.mapInfo.finalLevel:
                        self.logGame('GAME FAILED; likely..')
                    yield self.session
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

    def rebuildGameResult(self, deltaResult=None, sefResult=None):
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
        sessEscaped = self.session.gameEscapedAt is not None

        if deltaResult and sessEscaped:
            result['game_diff'] = deltaResult['game_diff']
            result['game_speed'] = deltaResult['game_speed']
            result['framework_version'] = None
            result['game_version'] = deltaResult['minor_version']
            result['escape_time'] = deltaResult['escape_time']
            result['escaped'] = deltaResult['escaped']
        elif sefResult and sessEscaped:
            result['game_diff'] = sefResult['game_diff']
            result['game_speed'] = sefResult['game_speed']
            result['framework_version'] = sefResult['framework_version']
            result['game_version'] = sefResult['game_version']
            result['escape_time'] = sefResult['escape_time']
            result['escaped'] = sefResult['escaped']
        else:
            result['game_diff'] = self.session.gameDiff
            result['game_speed'] = self.session.gameSpeed
            result['framework_version'] = 0
            result['game_version'] = 1
            result['escape_time'] = None
            if self.session.gameEscapedAt is not None:
                result['escape_time'] = round((self.session.gameEscapedAt - self.session.gameStartedAt) / self.timeFactor / 16.0, 2)
            result['escaped'] = True if result['escape_time'] is not None else None

        result['started_at_rt'] = None
        result['started_at_gt'] = round(self.session.gameStartedAt / 16.0, 2)

        if not sefResult:
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

                    if deltaResult and sessEscaped:
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
        else:
            result['players'] = copy.deepcopy(sefResult['players'])

        if not sefResult:
            result['challenges_completed'] = len(self.session.levels)
            result['challenges_total'] = len(self.mapInfo.levelRegions)

            result['challenges'] = OrderedDict()
            for chalId in self.session.levels:
                result['challenges'][chalId] = OrderedDict()
                result['challenges'][chalId]['completed_by'] = self.session.levels[chalId]['completed_by']
                if result['challenges'][chalId]['completed_by'] is not None and len(result['challenges'][chalId]['completed_by']) <= 0:
                    result['challenges'][chalId]['completed_by'] = [[15, 15]]
                result['challenges'][chalId]['buttons_by'] = []
                # TODO: buttons_by
                result['challenges'][chalId]['powerups_by'] = self.session.levels[chalId]['powerups_by']
                if self.session.levels[chalId]['completed_time'] is not None:
                    result['challenges'][chalId]['completed_time'] = round(self.session.levels[chalId]['completed_time'], 2)
                else:
                    result['challenges'][chalId]['completed_time'] = None
                result['challenges'][chalId]['time_offset_start'] = round(
                    (self.session.levels[chalId]['started_at'] - self.session.gameStartedAt) / (16.0 * self.timeFactor),
                    2
                )
                result['challenges'][chalId]['order'] = self.session.levels[chalId]['order']

            requiredLevels = filter(lambda x: True if 'optional' not in x else False, self.mapInfo.levelRegions.values())
            if len(self.session.levels) < len(requiredLevels) and result['escaped']:
                raise Exception('Levels completed count doesn\'t match with total count: [%d,%d]' % (len(self.session.levels), len(self.mapInfo.levelRegions)))
        else:
            result['challenges'] = copy.deepcopy(sefResult['challenges'])
            if sefResult['schema_version'] < 5 and self.session.gameEscapedAt:
                for chalId in result['challenges']:
                    result['challenges'][chalId]['completed_by'][0][1] = self.session.levels[chalId]['completed_by'][0][1]
                    for powerupKey, powerupItem in enumerate(result['challenges'][chalId]['powerups_by']):
                        if len(self.session.levels[chalId]['powerups_by']) > powerupKey:
                            result['challenges'][chalId]['powerups_by'][powerupKey][1] = self.session.levels[chalId]['powerups_by'][powerupKey][1]

        return result

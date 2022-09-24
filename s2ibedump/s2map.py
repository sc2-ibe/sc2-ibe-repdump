#!/usr/bin/python2
# -*- coding: utf-8 -*-

from __future__ import print_function
from pprint import pprint
import os
import re
import xml.etree.ElementTree as ET
import math


KNOWN_NAME_MAP = {
    'Ice Baneling Escape': 'IBE1',
    '도전! 맹독충의 빙판탈출': 'IBE1', # koKR
    '毒爆大逃亡': 'IBE1', # zhTW
    '雪地自爆虫大逃亡': 'IBE1', # zhCN

    'Reverse Ice Baneling Escape': 'RIBE1',

    'Ice Baneling Escape 2': 'IBE2',
    '도전! 맹독충의 빙판탈출 2': 'IBE2', # koKR

    'Ice Baneling Escape 2.1 - The Ice Awakens': 'IBE2.1',
    '맹독충의 빙판탈출 2.1 - 얼음 깨기': 'IBE2.1', # koKR - attempted name change
    '맹독충의 빙판탈출 2': 'IBE2.1', # koKR - original name

    'Ice Baneling Escape - Cold Voyage': 'IBE-CV',
    '맹독충의 빙판탈출 - 차가운 여행': 'IBE-CV', # koKR
    '雪地爆虫逃亡 - 冰霜远征': 'IBE-CV', # zhCN - probably incorrect?
    '雪地自爆虫逃亡 - 冰霜远征': 'IBE-CV', # zhCN

    'Ice Baneling Escape - EZ': 'IBE-CV-EZ',
    '맹독충의 빙판탈출 - 차가운 여행 - EZ': 'IBE-CV-EZ', # koKR
    '雪地爆虫逃亡 - 冰霜远征 EZ': 'IBE-CV-EZ', # zhCN - probably incorrect?

    'Ice Baneling Escape - Pro': 'IBE-CV-PRO',
    '맹독충의 빙판탈출 - 차가운 여행 - PRO': 'IBE-CV-PRO', # koKR
    '雪地爆虫逃亡 - 冰霜远征 PRO': 'IBE-CV-PRO', # zhCN

    'Back to Brood Ice Escape': 'BTB',
    'Back to Brood Ice Escape PRO': 'BTB-PRO',
    'Back to Brood Ice Escape PRO mode': 'BTB-PRO',
    '雪地爆虫逃亡 - 重回母巣 专家版': 'BTB-PRO', # zhCN
    'Back to Brood Baneling mode': 'BTB-BANE',
    'Back to Brood Ice Escape EZ': 'BTB-EZ',
    'Zealot dodge': 'Zealot',

    # UNOFFICIAL

    # Adu's test version
    'Ice Baneling Escape 1 - Comunity Version': 'IBE1',
    'Ice Baneling Escape 1 - Community Version': 'IBE1',

    # Talv's test version
    'IBE1 [T]': 'IBE1',
    'IBE2 [T]': 'IBE2',
}


class CmdFlags(object):
    Alternate              = 1 << 0
    Queued                 = 1 << 1
    Preempt                = 1 << 2
    SmartClick             = 1 << 3
    SmartRally             = 1 << 4
    Subgroup               = 1 << 5
    SetAutoCast            = 1 << 6
    SetAutoCastOn          = 1 << 7
    User                   = 1 << 8
    DataA                  = 1 << 9
    DataB                  = 1 << 10
    AI                     = 1 << 11
    AIIgnoreOnFinish       = 1 << 12
    IsOrder                = 1 << 13
    Script                 = 1 << 14
    HomogenousInterruption = 1 << 15
    Minimap                = 1 << 16
    Repeat                 = 1 << 17
    DispatchToOtherUnit    = 1 << 18
    TargetSelf             = 1 << 19
    Continuous             = 1 << 20
    QuickCast              = 1 << 21
    AutoQueued             = 1 << 22
    AIHeroes               = 1 << 23
    BuildOnSelf            = 1 << 24
    ToSelection            = 1 << 25


class RectangleShape(object):
    def __init__(self, minX, minY, maxX, maxY):
        self.minX = minX
        self.minY = minY
        self.maxX = maxX
        self.maxY = maxY

    def __repr__(self):
        return 'Rect[minX=%f minY=%f maxX=%f maxY=%f]' % (self.minX, self.minY, self.maxX, self.maxX)

    def getCenter(self):
        return {
            'x': (self.maxX + self.minX) / 2,
            'y': (self.maxY + self.minY) / 2,
        }

    def containsPoint(self, posX, posY):
        if self.minX > posX or self.maxX < posX:
            return False
        if self.minY > posY or self.maxY < posY:
            return False
        return True


class CircleShape(object):
    def __init__(self, centerX, centerY, radius):
        self.centerX = centerX
        self.centerY = centerY
        self.radius = radius

    def __repr__(self):
        return 'Circle[x=%f y=%f r=%f]' % (self.centerX, self.centerY, self.radius)

    def getCenter(self):
        return {
            'x': self.centerX,
            'y': self.centerY,
        }

    def containsPoint(self, posX, posY):
        if math.hypot(posX - self.centerX, posY - self.centerY) < self.radius:
            return True
        else:
            return False


class DiamondShape(object):
    def __init__(self, centerX, centerY, width, height):
        self.centerX = centerX
        self.centerY = centerY
        self.width = width
        self.height = height

    def __repr__(self):
        return 'Diamond[x=%f y=%f w=%f h=%f]' % (self.centerX, self.centerY, self.width, self.height)

    def getCenter(self):
        return {
            'x': self.centerX,
            'y': self.centerY,
        }

    def containsPoint(self, posX, posY):
        if math.hypot(posX - self.centerX, posY - self.centerY) < min([self.width, self.height]):
            return True
        else:
            return False


def readRegions(filename, gver):
    tree = ET.parse(filename)
    root = tree.getroot()

    def shapeFromRegion(region):
        shapeType = region.find('shape').attrib['type']
        shape = region.find('shape')
        if shapeType == 'rect':
            rg = RectangleShape(*map(
                lambda x: float(x),
                shape.find('quad').attrib['value'].split(',')
            ))
        elif shapeType == 'circle':
            center = map(
                lambda x: float(x),
                shape.find('center').attrib['value'].split(',')
            )
            rg = CircleShape(center[0], center[1], float(shape.find('radius').attrib['value']))
        elif shapeType == 'diamond':
            center = map(
                lambda x: float(x),
                shape.find('center').attrib['value'].split(',')
            )
            rg = DiamondShape(center[0], center[1], float(shape.find('width').attrib['value']), float(shape.find('height').attrib['value']))
        return rg

    def regionFromId(regId):
        for region in root:
            if int(region.attrib['id']) == regId:
                return shapeFromRegion(region)
        return None

    levels = {}
    if gver in ['IBE1', 'RIBE1']:
        for key in range(0, 21):
            levels[key] = {
                'finPlayers': lambda x: 1,
                'region': None,
                'spawn': None,
                'finish': None,
            }
    elif gver == 'IBE2':
        for key in range(0, 28):
            levels[key] = {
                'finPlayers': lambda x: 1,
                'region': None,
                'spawn': None,
                'finish': None,
            }
    elif gver in ['IBE-CV', 'IBE-CV-PRO']:
        levels[1] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(9),
            'spawn': regionFromId(1),
            'finish': regionFromId(2),
        }
        levels[2] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(193),
            'spawn': regionFromId(6),
            'finish': regionFromId(14),
        }
        levels[3] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(22),
            'spawn': regionFromId(7),
            'finish': regionFromId(15),
        }
        levels[4] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(23),
            'spawn': regionFromId(8),
            'finish': regionFromId(16),
        }
        levels[5] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(24),
            'spawn': regionFromId(10),
            'finish': regionFromId(17),
        }
        levels[6] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(25),
            'spawn': regionFromId(11),
            'finish': regionFromId(18),
        }
        levels[7] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(26),
            'spawn': regionFromId(12),
            'finish': regionFromId(19),
        }
        levels[8] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(27),
            'spawn': regionFromId(13),
            'finish': regionFromId(20),
        }
        levels[9] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(30),
            'spawn': regionFromId(29),
            'finish': regionFromId(28),
        }
        levels[10] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(33),
            'spawn': regionFromId(31),
            'finish': regionFromId(32),
        }
        levels[11] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(50),
            'spawn': regionFromId(48),
            'finish': regionFromId(49),
        }
        levels[12] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(57),
            'spawn': regionFromId(56),
            'finish': regionFromId(58),
        }
        levels[13] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(71),
            'spawn': regionFromId(70),
            'finish': regionFromId(62),
        }
        levels[14] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(75),
            'spawn': regionFromId(73),
            'finish': regionFromId(74),
        }
        levels[15] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(78),
            'spawn': regionFromId(76),
            'finish': regionFromId(77),
        }
        levels[16] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(88),
            'spawn': regionFromId(86),
            'finish': regionFromId(87),
        }
        levels[17] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(107),
            'spawn': regionFromId(108),
            'finish': regionFromId(109),
        }
        levels[18] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(115),
            'spawn': regionFromId(113),
            'finish': regionFromId(114),
        }
        levels[19] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(126),
            'spawn': regionFromId(125),
            'finish': regionFromId(124),
        }
        levels[20] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(154),
            'spawn': regionFromId(153),
            'finish': regionFromId(155),
        }
        levels[21] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(159),
            'spawn': regionFromId(157),
            'finish': regionFromId(158),
        }
        levels[22] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(165),
            'spawn': regionFromId(164),
            'finish': regionFromId(163),
        }
        levels[23] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(168),
            'spawn': regionFromId(167),
            'finish': regionFromId(166),
        }
        levels[24] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(173),
            'spawn': regionFromId(171),
            'finish': regionFromId(172),
        }
        levels[25] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(180),
            'spawn': regionFromId(178),
            'finish': regionFromId(179),
        }
        levels[26] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(34),
            'spawn': regionFromId(4),
            'finish': regionFromId(5),
        }
        levels[27] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(181),
            'spawn': regionFromId(130),
            'finish': regionFromId(182),
        }
        levels[28] = {
            'optional': True,
            'finPlayers': lambda x: 1,
            'region': regionFromId(238),
            'spawn': regionFromId(237),
            'finish': regionFromId(239),
        }
        return levels
    elif gver == 'IBE-CV-EZ':
        levels[1] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(9),
            'spawn': regionFromId(1),
            'finish': regionFromId(2),
        }
        levels[2] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(193),
            'spawn': regionFromId(6),
            'finish': regionFromId(14),
        }
        levels[3] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(25),
            'spawn': regionFromId(11),
            'finish': regionFromId(18),
        }
        levels[4] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(23),
            'spawn': regionFromId(8),
            'finish': regionFromId(16),
        }
        levels[5] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(24),
            'spawn': regionFromId(10),
            'finish': regionFromId(17),
        }
        levels[6] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(27),
            'spawn': regionFromId(13),
            'finish': regionFromId(20),
        }
        levels[7] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(30),
            'spawn': regionFromId(29),
            'finish': regionFromId(28),
        }
        levels[8] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(33),
            'spawn': regionFromId(31),
            'finish': regionFromId(32),
        }
        levels[9] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(57),
            'spawn': regionFromId(56),
            'finish': regionFromId(58),
        }
        levels[10] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(165),
            'spawn': regionFromId(164),
            'finish': regionFromId(163),
        }
        levels[11] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(168),
            'spawn': regionFromId(167),
            'finish': regionFromId(166),
        }
        levels[12] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(107),
            'spawn': regionFromId(108),
            'finish': regionFromId(109),
        }
        levels[13] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(115),
            'spawn': regionFromId(113),
            'finish': regionFromId(114),
        }
        levels[14] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(154),
            'spawn': regionFromId(153),
            'finish': regionFromId(155),
        }
        levels[15] = {
            'finPlayers': lambda x: 1,
            'region': regionFromId(181),
            'spawn': regionFromId(130),
            'finish': regionFromId(182),
        }
        return levels

    for region in root:
        regionName = region.find('name').attrib['value']
        lId = None
        regionKind = None

        if gver == 'IBE2' and regionName == 'lvl0FinalFFOpen':
            lId = 0
            regionKind = 'finish'
        elif gver == 'IBE2' and regionName == 'lvl0Reveal1':
            lId = 0
            regionKind = 'region'
        elif gver in ['IBE1', 'RIBE1']:
            if regionName == 'FinalLevelRegion':
                lId = 0
                regionKind = 'region'
            elif regionName == 'FinalLevelSpawnRegion':
                lId = 0
                regionKind = 'spawn'
            elif regionName == 'FinalLevelOpenSwitch05':
                lId = 0
                regionKind = 'finish'

        if lId is None:
            m = re.match(r'^(lvl|level)([0-9]+)(region|spawn|finish)(?:1p)?(?:region)?$', regionName, re.IGNORECASE)
            if not m:
                continue
            lId = m.groups()[1]
            regionKind = m.groups()[2]

        rg = shapeFromRegion(region)

        if gver == 'IBE2' and lId == '0607':
            levels[6][regionKind.lower()] = rg
            levels[7][regionKind.lower()] = rg
        else:
            levels[int(lId)][regionKind.lower()] = rg
            if gver == 'IBE2' and lId == '13':
                levels[27][regionKind.lower()] = rg

    if gver == 'IBE2':
        levels[0]['finPlayers'] = lambda x: 0
        levels[8]['finPlayers'] = lambda x: 2 if x>= 8 else 3 if x >= 5 else 1
        levels[9]['finPlayers'] = lambda x: 3 if x>= 3 else 2 if x >= 2 else 1
        levels[17]['finPlayers'] = lambda x: 2 if x >= 2 else 1
    elif gver in ['IBE1', 'RIBE1']:
        levels[0]['finPlayers'] = lambda x: 0

    return levels


class MapInfo(object):
    def __init__(self, mapId):
        self.mapId = mapId
        fname = mapId.lower()
        if mapId == 'IBE-CV-PRO':
            fname = 'ibe-cv'
        self.levelRegions = readRegions(os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'data/%s/Regions' % fname
        ), mapId)
        self.obstaclePlayerId = 15
        if mapId.startswith('IBE-CV'):
            self.obstaclePlayerId = 0

        self.finalLevel = None
        if self.mapId in ['IBE1', 'IBE2', 'RIBE1']:
            self.finalLevel = 0
        elif self.mapId in ['IBE-CV', 'IBE-CV-PRO']:
            self.finalLevel = 27
        elif self.mapId in ['IBE-CV-EZ']:
            self.finalLevel = 15

    def findClosestLevel(self, regionName, posX, posY):
        currLvlId = None
        currDistance = None
        for key in self.levelRegions:
            tmpCenter = self.levelRegions[key][regionName].getCenter()
            tmpDistance = math.hypot(posX - tmpCenter['x'], posY - tmpCenter['y'])
            if not currDistance or tmpDistance < currDistance:
                currLvlId = key
                currDistance = tmpDistance
        return currLvlId

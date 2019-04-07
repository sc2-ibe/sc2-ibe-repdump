#!/usr/bin/python2
# -*- coding: utf-8 -*-

from __future__ import print_function
from pprint import pprint
import os
import re
import xml.etree.ElementTree as ET
import math


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


def readRegions(filename, gver):
    tree = ET.parse(filename)
    root = tree.getroot()

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

        rg = None
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
        self.levelRegions = readRegions(os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'data/%s/Regions' % mapId.lower()
        ), mapId)

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

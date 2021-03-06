# -*- coding: utf-8 -*-


# OpenFisca -- A versatile microsimulation software
# By: OpenFisca Team <contact@openfisca.fr>
#
# Copyright (C) 2011, 2012, 2013, 2014 OpenFisca Team
# https://github.com/openfisca
#
# This file is part of OpenFisca.
#
# OpenFisca is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# OpenFisca is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import collections
import datetime

from biryani1 import strings
import numpy as np

from . import conv
from .enumerations import Enum


# Base Column


class Column(object):
    _default = 0
    _dtype = float
    cerfa_field = None
    end = None
    entity = None
    info = None
    # json_type = None  # Defined in sub-classes
    label = None
    legislative_input = False
    name = None
    start = None
    survey_only = False
    val_type = None
    cerfa_field = None
    info = None

    def __init__(self, label = None, cerfa_field = None, default = None, end = None, entity = None, info = None,
            legislative_input = True, start = None, survey_only = False, val_type = None):
        if cerfa_field is not None:
            self.cerfa_field = cerfa_field
        if default is not None and default != self._default:
            self._default = default
        if end is not None:
            self.end = end
        self.entity = entity or 'ind'
        if info is not None:
            self.info = info
        if label is not None:
            self.label = label
        if legislative_input:
            self.legislative_input = True
        if start is not None:
            self.start = start
        if survey_only:
            self.survey_only = True
        if val_type is not None and val_type != self.val_type:
            self.val_type = val_type
        if cerfa_field is not None:
            self.cerfa_field = cerfa_field
        if info is not None:
            self.info = info

    def to_json(self):
        self_json = collections.OrderedDict((
            ('@type', self.json_type),
            ))
        if self.cerfa_field is not None:
            self_json['cerfa_field'] = self.cerfa_field
        if self._default is not None:
            self_json['default'] = self._default
        end = self.end
        if end is not None:
            if isinstance(end, datetime.date):
                end = end.isoformat()
            self_json['end'] = end
        if self.entity is not None:
            self_json['entity'] = self.entity
        if self.freq != 'year':
            self_json['freq'] = self.freq
        if self.label is not None:
            self_json['label'] = self.label
        if self.name is not None:
            self_json['name'] = self.name
        start = self.start
        if start is not None:
            if isinstance(start, datetime.date):
                start = start.isoformat()
            self_json['start'] = start
        if self.val_type is not None:
            self_json['val_type'] = self.val_type
        return self_json


# Level-1 Columns


class BoolCol(Column):
    '''
    A column of boolean
    '''
    _default = False
    _dtype = np.bool
    json_type = 'Boolean'

    @property
    def json_to_python(self):
        return conv.pipe(
            conv.test_isinstance(int, bool),
            conv.anything_to_bool,
            )


class DateCol(Column):
    '''
    A column of Datetime 64 to store dates of people
    '''
    _dtype = np.datetime64
    json_type = 'Date'
    val_type = 'date'

    @property
    def json_to_python(self):
        return conv.pipe(
            conv.test_isinstance(basestring),
            conv.iso8601_input_to_date,
            )


class FloatCol(Column):
    '''
    A column of float 32
    '''
    _dtype = np.float32
    json_type = 'Float'

    @property
    def json_to_python(self):
        return conv.pipe(
            conv.test_isinstance(float, int),
            conv.anything_to_float,
            )


class IntCol(Column):
    '''
    A column of integer
    '''
    _dtype = np.int32
    json_type = 'Integer'

    @property
    def json_to_python(self):
        return conv.test_isinstance(int)


# Level-2 Columns


class AgesCol(IntCol):
    '''
    A column of Int to store ages of people
    '''
    _default = -9999

    @property
    def json_to_python(self):
        return conv.pipe(
            super(AgesCol, self).json_to_python,
            conv.first_match(
                conv.test_greater_or_equal(0),
                conv.test_equals(-9999),
                ),
            )


class EnumCol(IntCol):
    '''
    A column of integer with an enum
    '''
    _dtype = np.int16
    enum = None
    index_by_slug = None
    json_type = 'Enumeration'

    def __init__(self, enum = None, **kwargs):
        super(EnumCol, self).__init__(**kwargs)
        if isinstance(enum, Enum):
            self.enum = enum

    @property
    def json_to_python(self):
        enum = self.enum
        if enum is None:
            return conv.pipe(
                conv.test_isinstance((basestring, int)),
                conv.anything_to_int,
                conv.default(self._default),
                )
        # This converters accepts either an item number or an item name.
        index_by_slug = self.index_by_slug
        if index_by_slug is None:
            self.index_by_slug = index_by_slug = dict(
                (strings.slugify(name), index)
                for index, name in sorted(enum._vars.iteritems())
                )
        return conv.pipe(
            conv.test_isinstance((basestring, int)),
            conv.condition(
                conv.anything_to_int,
                conv.pipe(
                    # Verify that item index belongs to enumeration.
                    conv.anything_to_int,
                    conv.test_in(enum._vars),
                    ),
                conv.pipe(
                    # Convert item name to its index.
                    conv.input_to_slug,
                    conv.test_in(index_by_slug),
                    conv.function(lambda slug: index_by_slug[slug]),
                    ),
                ),
            conv.default(
                self._default
                if self._default is not None and self._default in enum._vars
                else min(enum._vars.iterkeys())
                ),
            )

    def to_json(self):
        self_json = super(EnumCol, self).to_json()
        if self.enum is not None:
            self_json['labels'] = collections.OrderedDict(
                (index, label)
                for label, index in self.enum
                )
        return self_json


# Base Prestation


class Prestation(Column):
    """
    Prestation is a wraper around a function which takes some arguments and return a single array.
    _P is a reserved kwargs intended to pass a tree of parametres to the function
    """
    _func = None
    formula_constructor = None

    def __init__(self, func, entity = None, label = None, start = None, end = None, val_type = None, freq = None,
            survey_only = False):
        super(Prestation, self).__init__(label = label, entity = entity, start = start, end = end, val_type = val_type,
            freq = freq, survey_only = survey_only)

        self._children = set()  # prestations immediately affected by current prestation
        self._freq = {}
        assert func is not None, 'A function to compute the prestation should be provided'
        self._func = func


# Level-1 Prestations


class BoolPresta(Prestation, BoolCol):
    '''
    A Prestation inheriting from BoolCol
    '''
    def __init__(self, func, **kwargs):
        BoolCol.__init__(self, **kwargs)
        Prestation.__init__(self, func = func, **kwargs)


class EnumPresta(Prestation, EnumCol):
    '''
    A Prestation inheriting from EnumCol
    '''
    def __init__(self, func, enum = None, **kwargs):
        EnumCol.__init__(self, enum = enum, **kwargs)
        Prestation.__init__(self, func, **kwargs)


class FloatPresta(Prestation, FloatCol):
    '''
    A Prestation inheriting from BoolCol
    '''
    def __init__(self, func, **kwargs):
        FloatCol.__init__(self, **kwargs)
        Prestation.__init__(self, func = func, **kwargs)


class IntPresta(Prestation, IntCol):
    '''
    A Prestation inheriting from IntCol
    '''
    def __init__(self, func, **kwargs):
        IntCol.__init__(self, **kwargs)
        Prestation.__init__(self, func, **kwargs)

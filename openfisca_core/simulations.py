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


<<<<<<< HEAD
import collections
import copy
import datetime as dt
import gc
import itertools
import os
import pickle
import sys
from xml.etree import ElementTree

from pandas import DataFrame, HDFStore

from . import conv, legislations, legislationsxml, model
from .columns import EnumCol, EnumPresta
from .datatables import DataTable
from .taxbenefitsystems import TaxBenefitSystem


__all__ = ['ScenarioSimulation', 'Simulation', 'SurveySimulation']


check_consistency = None  # Set to a function by country-specific package


class Simulation(object):
    """
    A simulation object contains all parameters to compute a simulation from a
    test-case household (scenario) or a survey-like dataset

    See also
    --------
    ScenarioSimulation, SurveySimulation
    """
    chunks_count = 1
    column_by_name = None
    datesim = None
    disabled_prestations = None
    input_table = None
    io_column_by_label = None
    io_column_by_name = None
    num_table = 1
    output_table = None
    output_table_default = None
    P = None
    P_default = None
    param_file = None
    prestation_by_name = None
    reforme = False  # Boolean signaling reform mode
    subset = None
    verbose = False

    def __init__(self):
        self.io_column_by_label = collections.OrderedDict()
        self.io_column_by_name = collections.OrderedDict()

    def __getstate__(self):
        def should_pickle(v):
            return v not in ['P_default', 'P']
        return dict((k, v) for (k, v) in self.__dict__.iteritems() if should_pickle(k))

    def _set_config(self, **kwargs):
        """
        Sets some general Simulation attributes
        """
        remaining = kwargs.copy()

        for key, val in kwargs.iteritems():
            if key == "year":
                date_str = str(val)+ '-01-01'
                self.datesim = dt.datetime.strptime(date_str ,"%Y-%m-%d").date()
                remaining.pop(key)

            elif key == "datesim":
                if isinstance(val, dt.date):
                    self.datesim = val
                else:
                    self.datesim = dt.datetime.strptime(val ,"%Y-%m-%d").date()
                remaining.pop(key)

            elif key in ['param_file', 'decomp_file']:
                if hasattr(self, key):
                    setattr(self, key, val)
                    remaining.pop(key)

        if self.param_file is None:
            self.param_file = model.PARAM_FILE

        # Sets required country specific classes
        self.column_by_name = model.column_by_name
        self.prestation_by_name = model.prestation_by_name

        return remaining

    def set_param(self, param=None, param_default=None):
        """
        Set the parameters of the simulation

        Parameters
        ----------
        param : a socio-fiscal parameter object to be used in the microsimulation.
                By default, the method uses the one provided by the attribute param_file
        param_default : a socio-fiscal parameter object to be used
                in the microsimulation to compute some gross quantities not available in the initial data.
                parma_default is necessarily different from param when examining a reform
        """
        if param is None or param_default is None:
            legislation_tree = ElementTree.parse(self.param_file)
            legislation_xml_json = conv.check(legislationsxml.xml_legislation_to_json)(legislation_tree.getroot(),
                state = conv.default_state)
            legislation_xml_json, _ = legislationsxml.validate_node_xml_json(legislation_xml_json,
                state = conv.default_state)
            _, legislation_json = legislationsxml.transform_node_xml_json_to_json(legislation_xml_json)
            dated_legislation_json = legislations.generate_dated_legislation_json(legislation_json, self.datesim)
            compact_legislation = legislations.compact_dated_node_json(dated_legislation_json)

        if param_default is None:
            self.P_default = copy.deepcopy(compact_legislation)
        else:
            self.P_default = param_default

        if param is None:
            self.P = compact_legislation
        else:
            self.P = param

    def _initialize_input_table(self):
        self.input_table = DataTable(self.column_by_name, datesim=self.datesim, num_table = self.num_table,
            subset=self.subset, print_missing=self.verbose)

    def disable_prestations(self, disabled_prestations = None):
        """
        Disable some prestations that will remain to their default value

        Parameters
        ----------
        disabled_prestations : list of strings, default None
                               names of the prestations to be disabled
        """
        self.disabled_prestations = disabled_prestations

    def _preproc(self):
        """
        Prepare the output values according to the prestation_by_name definitions/Reform status/input_table

        Parameters
        ----------

        input_table : DataTable

        Returns
        -------

        output, output_table_default : TaxBenefitSystem
                                 DataTable of the output variable of the socio-fiscal model
        """
        P, P_default = self.P, self.P_default
        input_table = self.input_table

        output_table = TaxBenefitSystem(self.prestation_by_name, P, P_default, datesim = P.datesim,
            num_table = self.num_table)
        output_table.set_inputs(input_table)

        if self.reforme:
            output_table_default = TaxBenefitSystem(self.prestation_by_name, P_default, P_default, datesim = P.datesim,
                num_table = self.num_table)
            output_table_default.set_inputs(input_table)
        else:
            output_table_default = output_table

        output_table.disable(self.disabled_prestations)
        output_table_default.disable(self.disabled_prestations)

        io_column_by_label = self.io_column_by_label
        io_column_by_name = self.io_column_by_name
        io_column_by_label.clear()
        io_column_by_name.clear()
        for column_name, column in itertools.chain(
                input_table.column_by_name.iteritems(),
                output_table.column_by_name.iteritems(),
                ):
            io_column_by_label[column.label] = column
            io_column_by_name[column_name] = column

        self.output_table, self.output_table_default = output_table, output_table_default

    def _compute(self, **kwargs):
        """
        Computes output_data for the Simulation

        Parameters
        ----------
        difference : boolean, default True
                     When in reform mode, compute the difference between actual and default
        Returns
        -------
        data, data_default : Computed data and possibly data_default according to decomp_file

        """
        # Clear outputs
        #self.clear()

        self._preproc()
        output_table, output_table_default = self.output_table, self.output_table_default

        for key, val in kwargs.iteritems():
            setattr(output_table, key, val)
            setattr(output_table_default, key, val)

        data = output_table.calculate()
        if self.reforme:
            output_table_default.reset()
            output_table_default.disable(self.disabled_prestations)
            data_default = output_table_default.calculate()
        else:
            output_table_default = output_table
            data_default = data

        self.data, self.data_default = data, data_default

        io_column_by_label = self.io_column_by_label
        io_column_by_name = self.io_column_by_name
        for column_name, column in output_table.column_by_name.iteritems():
            io_column_by_label[column.label] = column
            io_column_by_name[column_name] = column

        gc.collect()

    def clear(self):
        """
        Clear the output_table
        """
        self.output_table = None
        self.output_table_default = None
        gc.collect()

    def get_col(self, varname):
        '''
        Look for a column in input_table column_by_name, then in output_table column_by_name.
        '''
        if varname in self.input_table.column_by_name:
            return self.input_table.column_by_name[varname]
        if self.output_table is not None and varname in self.output_table.column_by_name:
            return self.output_table.column_by_name[varname]
        print "Variable %s is absent from both input_table and output_table" % varname
        return None

    @property
    def input_var_list(self):
        """
        List of input survey variables
        """
        if self.input_table is None:
            self._initialize_input_table()
        return self.input_table.column_by_name.keys()

    @property
    def output_var_list(self):
        """
        List of output survey variables
        """
        if self.output_table is not None:
            return self.output_table.column_by_name.keys()

    @property
    def var_list(self):
        """
        List the variables present in survey and output
        """
        if self.input_table is None:
            return None
        columns_name = set(self.input_table.column_by_name)
        if self.output_table is not None:
            columns_name.update(self.output_table.column_by_name)
        return list(columns_name)

    def create_description(self):
        '''
        Creates a description dataframe of the ScenarioSimulation
        '''
        now = dt.datetime.now()
        descr =  [u'OpenFisca',
                         u'Calculé le %s à %s' % (now.strftime('%d-%m-%Y'), now.strftime('%H:%M')),
                         u'Système socio-fiscal au %s' % str(self.datesim)]
        # TODO: add other parameters
        return DataFrame(descr)

    def save_content(self, name, filename):
        """
        Saves content from the simulation in an HDF store.
        We save output_table, input_table, and the default output_table dataframes,
        along with the other attributes using pickle.
        TODO : we don't save attributes P, P_default for simulation
                neither _param, _default_param for datatables.
        WARNING : Be careful when committing, you may have created a .pk data file.

        Parameters
        ----------
        name : the base name of the content inside the store.

        filename : the name of the .h5 file where the table is stored. Created if not existant.
        """

        sys.setrecursionlimit(32000)
        # Store the tables
        if self.verbose:
            print 'Saving content for simulation under name %s' %name
        ERF_HDF5_DATA_DIR = os.path.join(model.DATA_DIR, 'erf')
        store = HDFStore(os.path.join(os.path.dirname(ERF_HDF5_DATA_DIR),filename+'.h5'))
        if self.verbose:
            print 'Putting output_table in...'
        store.put(name + '_output_table', self.output_table.table)
        if self.verbose:
            print 'Putting input_table in...'
        store.put(name + '_input_table', self.input_table.table)
        if self.verbose:
            print 'Putting output_table_default in...'
        store.put(name + '_output_table_default', self.output_table_default.table)

        store.close()

        # Store all attributes from simulation
        with open(filename + '.pk', 'wb') as output:
            if self.verbose:
                print 'Storing attributes for simulation (including sub-attributes)'
            pickle.dump(self, output)


class ScenarioSimulation(Simulation):
    """
    A Simulation class tailored to deal with scenarios
    """
    alternative_scenario = None
    data = None
    data_default = None
    decomp_file = None
    maxrev = None
    mode = None
    nmen = None
    same_rev_couple = False
    Scenario = None
    scenario = None
    x_axis = None

    def set_config(self, **kwargs):
        """
        Configures the ScenarioSimulation

        Parameters
        ----------
        scenario : a scenario (by default, None selects Scenario())
        param_file : the socio-fiscal parameters file
        decomp_file : the decomp file
        x_axis : the revenue category along which revenue varies
        maxrev : the maximal value of the revenue
        same_rev_couple : divide the revenue equally between the two partners
        mode : 'bareme' or 'castype' TODO: change this
        """

        specific_kwargs = self._set_config(**kwargs)
        self.Scenario = model.Scenario
        if self.scenario is None:
            self.scenario = kwargs.get('scenario') or self.Scenario()

        self.scenario.year = self.datesim.year

        for key, val in specific_kwargs.iteritems():
            if hasattr(self, key):
                setattr(self, key, val)

        self.scenario.nmen   = self.nmen
        self.scenario.maxrev = self.maxrev
        self.scenario.x_axis  = self.x_axis
        self.scenario.same_rev_couple  = self.same_rev_couple

        if self.decomp_file is None:
            self.decomp_file = os.path.join(model.DECOMP_DIR, model.DEFAULT_DECOMP_FILE)
        elif not os.path.exists(self.decomp_file):
            self.decomp_file = os.path.join(model.DECOMP_DIR, self.decomp_file)

        self.initialize_input_table()

    def get_varying_revenues(self, var):
        """
        List the potential varying revenues
        """
        x_axis = model.x_axes.get(var)
        if x_axis is None:
            raise Exception("No revenue for variable %s" %(var))
        return x_axis.typ_tot_default

    def reset_scenario(self):
        """
        Reset scenario and alternative_scenario to their default values
        """
        if self.Scenario is not None:
            self.scenario = self.Scenario()
        self.alternative_scenario = None

    def set_alternative_scenario(self, scenario):
        """
        Set alternative-scenario

        Parameters
        ----------
        scenario : an instance of the class Scenario
        """
        self.alternative_scenario = scenario

    def set_marginal_alternative_scenario(self, entity = None, id_in_entity = None, variable = None, value = None):
        """
        Modifies scenario by changing the setting value of the variable of the individual with
        position 'id' if necessary in entity named 'entity'
        """
        self.alternative_scenario = self.scenario.copy()
        scenario = self.alternative_scenario
        if entity is not None:
            alt_entity = getattr(scenario, entity)
            if id_in_entity is not None:
                alt_entity[id_in_entity][variable] = value

    def initialize_input_table(self):
        """
        Initializee the input table of the ScenarioSimulation
        """
        self._initialize_input_table()

    def compute(self, difference=False):
        """
        """
        self.input_table.load_data_from_test_case(self.scenario)
        self._preproc()

        alter = self.alternative_scenario is not None
        if self.reforme and alter:
            raise Exception("ScenarioSimulation: 'self.reforme' cannot be 'True' when 'self.alternative_scenario' is not 'None'")

        if alter:
            input_table_alter = DataTable(self.column_by_name, datesim = self.datesim)
            input_table_alter.load_data_from_test_case(self.alternative_scenario)

        self._compute(decomp_file=self.decomp_file)
        if alter:
            output_table = TaxBenefitSystem(self.prestation_by_name, self.P, self.P_default, datesim = self.P.datesim,
                num_table = self.num_table)
            output_table.set_inputs(input_table_alter)
            output_table.decomp_file = self.decomp_file
            output_table.disable(self.disabled_prestations)
            self.data = output_table.calculate()
            self.output_table = output_table

        if difference or self.reforme:
            self.data.difference(self.data_default)

        return self.data, self.data_default

    def preproc_alter_scenario(self, input_table_alter):
        """
        Prepare the output values according to the prestation_by_name definitions and
        input_table when an alternative scenario is present

        Parameters
        ----------

        input_table_alter : TODO: complete
        input_table : TODO: complete
        """
        P_default = self.P_default
        P         = self.P

        self.output_table = TaxBenefitSystem(self.prestation_by_name, P, P_default, datesim = P.datesim)
        self.output_table.set_inputs(self.input_table)

        output_alter = TaxBenefitSystem(self.prestation_by_name, P, P_default, datesim = P.datesim)
        output_alter.set_inputs(input_table_alter)

        self.output_table.disable(self.disabled_prestations)
        output_alter.disable(self.disabled_prestations)

        return output_alter, self.output_table

    def get_results_dataframe(self, default = False, difference = False, index_by_code = False):
        """
        Formats data into a dataframe

        Parameters
        ----------
        default : boolean, default False
                  If True compute the default results
        difference :  boolean, default True
                      If True compute the difference between actual and default results
        index_by_code : boolean, default False
                  Index the row by the code instead of name of the different element
                  of decomp_file

        Returns
        -------
        df : A DataFrame with computed data according to decomp_file
        """
        if self.data is None:
            self.compute(difference = difference)

        data = self.data
        data_default = self.data_default

        data_dict = dict()
        index = []

        if default is True:
            data = data_default

        for row in data:
            if not row.desc in ('root'):
                if index_by_code is True:
                    index.append(row.code)
                    data_dict[row.code] = row.vals
                else:
                    index.append(row.desc)
                    data_dict[row.desc] = row.vals

        df = DataFrame(data_dict).T
        df = df.reindex(index)
        return df


class SurveySimulation(Simulation):
    """
    A Simulation class tailored to deal with survey data
    """
    descr = None
    survey_filename = None

    def set_config(self, **kwargs):
        """
        Configures the SurveySimulation

        Parameters
        ----------
        TODO:
        survey_filename
        num_table
        """
        # Setting general attributes and getting the specific ones
        specific_kwargs = self._set_config(**kwargs)

        for key, val in specific_kwargs.iteritems():
            if hasattr(self, key):
                setattr(self, key, val)

        if self.survey_filename is None:
            if self.num_table == 1 :
                filename = os.path.join(model.DATA_DIR, 'survey.h5')
            else:
                filename = os.path.join(model.DATA_DIR, 'survey3.h5')

            self.survey_filename = filename

        if self.num_table not in [1,3] :
            raise Exception("OpenFisca can be run with 1 or 3 tables only, "
                            " please, choose between both.")

        if not isinstance(self.chunks_count, int):
            raise Exception("Chunks count must be an integer")

    def inflate_survey(self, inflators):
        """
        Inflate some variable of the survey data

        Parameters
        ----------
        inflators : dict or DataFrame
                    keys or a variable column should contain the variables to
                    inflate and values of the value column the value of the inflator
        """

        if self.input_table is None:
            self.initialize_input_table()
            self.input_table.load_data_from_survey(self.survey_filename,
                                               num_table = self.num_table,
                                               subset=self.subset,
                                               print_missing=self.verbose)

        if isinstance(inflators, DataFrame):
            for varname in inflators['variable']:
                inflators.set_index('variable')
                inflator = inflators.get_value(varname, 'value')
                self.input_table.inflate(varname, inflator)
        if isinstance(inflators, dict):
            for varname, inflator in inflators.iteritems():
                self.input_table.inflate(varname, inflator)

    def check_input_table(self):
        """
        Consistency check of survey input data
        """
        check_consistency(self.input_table)

    def initialize_input_table(self):
        """
        Initialize the input_table for a survey based simulation
        """
        self.clear()
        self._initialize_input_table()

        io_column_by_label = self.io_column_by_label
        io_column_by_name = self.io_column_by_name
        io_column_by_label.clear()
        io_column_by_name.clear()
        for column_name, column in self.input_table.column_by_name.iteritems():
            io_column_by_label[column.label] = column
            io_column_by_name[column_name] = column

    def compute(self):
        """
        Computes the output_table for a survey based simulation
        """
        self.clear()
        if self.input_table is None:
            self.initialize_input_table()
            self.input_table.load_data_from_survey(self.survey_filename,
                                               num_table = self.num_table,
                                               subset=self.subset,
                                               print_missing=self.verbose)

        if self.chunks_count == 1:
            self._compute()
        # Note: subset has already be applied
=======
class Simulation(object):
    compact_legislation = None
    date = None
    default_compact_legislation = None
    entities = None
    entity_by_column_name = None
    tax_benefit_system = None

    def __init__(self, compact_legislation = None, date = None, tax_benefit_system = None):
        assert date is not None
        self.date = date
        assert tax_benefit_system is not None
        self.tax_benefit_system = tax_benefit_system

        self.compact_legislation = compact_legislation \
            if compact_legislation is not None \
            else tax_benefit_system.get_compact_legislation(date)
        self.default_compact_legislation = tax_benefit_system.get_compact_legislation(date)

    def compute(self, column_name, requested_columns_name = None):
        if requested_columns_name is None:
            requested_columns_name = set()
>>>>>>> cadc11a7309a850c8e968443201431d5f51080f6
        else:
            assert column_name not in requested_columns_name, 'Infinite loop. Missing values for columns: {}'.format(
                u', '.join(sorted(requested_columns_name)).encode('utf-8'))
        return self.entity_by_column_name[column_name].compute(column_name, requested_columns_name)

    def set_entities(self, entities):
        self.entities = entities
        self.entity_by_column_name = dict(
            (column_name, entity)
            for entity in self.entities.itervalues()
            for column_name in entity.column_by_name.iterkeys()
            )

    def get_holder_by_name(self, column_name, default = UnboundLocalError):
        entity = self.entity_by_column_name[column_name]
        if default is UnboundLocalError:
            return entity.holder_by_name[column_name]
        return entity.holder_by_name.get(column_name, default)

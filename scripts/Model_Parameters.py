import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, MetaData, select, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import geopy.distance
import datetime


"""
This file contains all the classes and methods used to calculate the costs of components required for a farming system. 
They are organised into classes based on the type of use that they contain. 
"""


class FarmInputs:

    def __init__(self, **kwargs):

        #TODO Call dataframes from SQL database once values are accurate
        fp = 'C:/Users/risid/Google Drive/Python/land-use-models/data/mamiro_cost_estimates/pricing_database.xlsx'
        fp2 = 'C:/Users/risid/Google Drive/Python/land-use-models/LUCM_Master_File.xlsx'

        #### FARM INPUTS ####

        self.land_use_df = pd.read_excel(fp2, sheet_name='Land_Use_Master_List').set_index('Land Use Class')
        self.cost_df = pd.read_excel(fp, sheet_name='Database')
        self.cost_df.set_index('item', inplace=True)
        self.cost_dict = self.cost_df.to_dict('index')

        self.ucl = 'unit_cost_low'
        self.uch = 'unit_cost_high'
        self.bcl = 'base_cost_low'
        self.bch = 'base_cost_low'

        # Farm Location #

        self.lat = kwargs.get('farm_lat', 0)
        self.long = kwargs.get('farm_long', 0)

        # Initial Land Use Values
        self.land_use_current = kwargs.get('current_land_use','NA')
        self.land_use_proposed = kwargs.get('proposed_land_use', 'NA')

        # Set Default values for a variety of inputs for various land uses

        self.pad_size_current = kwargs.get('current_paddock_size', self.land_use_df.at[self.land_use_current, 'Default_Pad_Size'])
        self.pad_size_proposed = kwargs.get('proposed_paddock_size',
                                            self.land_use_df.at[self.land_use_proposed, 'Default_Pad_Size'])
        self.land_area = kwargs.get('land_area', 'NA')
        #TODO add functionality to break up the total land area fir multiple land uses

        # Lets user specify how long relative to wide the paddock and farm will be
        self.pad_length = kwargs.get('pad_length_to_width', 2)
        self.farm_length = kwargs.get('farm_length_to_width', 2)

        self.boundary_fence_perimeter = np.sqrt(self.land_area / self.farm_length)
        self.paddock_perimeter = np.sqrt(self.pad_size_proposed / self.pad_length)

        self.boundary_width = (self.boundary_fence_perimeter / (self.farm_length + 1))
        self.boundary_length = self.boundary_width * self.farm_length

        self.pad_width = (self.paddock_perimeter / (self.pad_length + 1))
        self.pad_length = self.pad_width * self.pad_length


        #### LIVESTOCK INPUTS ####
        self.stock_type_current = kwargs.get('stock_type', 'cow')
        self.stock_milking = kwargs.get('milking_cows', 100)
        self.stock_dry = kwargs.get('dry_cows', 100)
        self.stock_heifers = kwargs.get('cows_heifers', 100)
        self.stock_LWT = kwargs.get('stock_weight', 475)
        self.MS_per_cow = kwargs.get('MS_per_year_per_stock', 450)
        self.lactation_days = kwargs.get('lactation_days', 300)
        self.milk_start_month = kwargs.get('start_milking_month', 7)
        self.milk_start_day = kwargs.get('start_milking_day', 7)
        self.milking_time = kwargs.get('milking_time',4) # hours per day
        self.imported_feed_perc = kwargs.get('perc_imported_feed', 35)

        #### FERTILISER INPUTS ####
        self.ave_N_app = kwargs.get('N_appliction', 220) # kg/ha/year
        self.ave_k_app = kwargs.get('K_application', 70) # kg/ha/year

        #### MILKING SHED INPUTS ####
        self.washdown_water_per_cow = kwargs.get('washdown_water_per_cow', 35) #l/cow/day
        self.mm_wash_water = kwargs.get('mm_wash_water',4) #m3/day
        self.yard_area = kwargs.get('yard_area','NA')
        self.sw_divert = kwargs.get('stormwater_divert', True)
        self.other_area = kwargs.get('other_area','NA')

        #### OTHER INFRASTRUCTURE ####
        self.feed_pad = kwargs.get('feed_pad', False)
        self.feed_pad_area = kwargs.get('feed_pad_area', 'NA')

        #### SET UP DATABASE CONNECTION ####
        self.db_name = kwargs.get('db_name', 'sandpit')
        self.db_user_name = kwargs.get('db_user name', "postgres")
        self.db_user_pw = kwargs.get('db_user password', '&MaM!r0postgres&')
        self.main_table = kwargs.get('main_table','NA')

        host = "127.0.0.1"
        port = "5432"
        driver = 'psycopg2'
        db_type = 'postgresql'

        self.db_string = '%s+%s://%s:%s@%s:%s/%s' % (db_type,
                                                     driver,
                                                     self.db_user_name,
                                                     self.db_user_pw,
                                                     host,
                                                     port,
                                                     self.db_name)

        self.engine = create_engine(self.db_string, pool_size=50, max_overflow=0)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.Base = declarative_base()
        # Look up the existing tables from database
        self.Base.metadata.reflect(self.engine)

        #TODO develop formula to estimate yard area based on rotary / herringbone and cow numbers


class Fences:

    def __init__(self, fi_object):

        # Calcualte length of fences
        self.FI = fi_object
        self.boundary_fence_length = 2 * fi_object.boundary_length + 2 * fi_object.boundary_width
        self.gate_fence_length = (fi_object.boundary_width / fi_object.pad_length - 1) * fi_object.boundary_width
        self.internal_fence_length = (fi_object.boundary_length / fi_object.pad_width - 1) * fi_object.boundary_length

        # Other items that might be of use TODO put thin in the other costs
        self.internal_gates = (fi_object.boundary_width / (fi_object.pad_length * 2) - 1) / fi_object.pad_width

    def fence_cost(self, **kwargs):

        type_ = kwargs.get('type',"NA")
        length = kwargs.get('length',"NA")
        contractor = kwargs.get('contractor', False)
        #TODO turn below if statements into a dictionary to make it tidier
        if type_ == 'Post & Batton - 2 Wires Electric':

            fence_cost = Fences.fence_calc(self,
                                           length,
                                           type_=type_,
                                           contractor=contractor,
                                           no_wires=5,
                                           battons=True,
                                           batton_spacing=1,
                                           post_type='Quarter',
                                           post_spacing=5,
                                           electric_wires=2
                                           )

        elif type_ == 'Post & Batton - 3 Wires Electric':

            fence_cost = Fences.fence_calc(self, length,
                                           type_=type_,
                                           contractor=contractor,
                                           no_wires=7,
                                           battons=True,
                                           batton_spacing=1,
                                           post_type='Quarter',
                                           post_spacing=5,
                                           electric_wires=3
                                           )

        elif type_ == '3 Wire - Quarter Round':

            fence_cost = Fences.fence_calc(self, length,
                                           type_=type_,
                                           contractor=contractor,
                                           no_wires=3,
                                           battons=False,
                                           batton_spacing=0,
                                           post_type='Quarter',
                                           post_spacing=5,
                                           electric_wires=3
                                           )

        elif type_ == '4 Wire - Quarter Round':

            fence_cost = Fences.fence_calc(self, length,
                                           type_=type_,
                                           contractor=contractor,
                                           no_wires=4,
                                           battons=False,
                                           batton_spacing=0,
                                           post_type='Quarter',
                                           post_spacing=5,
                                           electric_wires=3
                                           )

        else:

            pass

        return fence_cost

    def fence_calc(self, length, **kwargs):

        # TODO Add cost of corners, strainers and tightners and gates.

        # INPUT
        # fence_type - kwargs.get('fence_type', 'Stock')
        no_wires = kwargs.get('no_wires', "NA")
        battons = kwargs.get('battons', False)
        batton_spacing = kwargs.get('batton_spacing', 1)
        post_type = kwargs.get('post_type', 'Quarter')
        electric_wires = kwargs.get('electric_wires', 1)
        post_spacing = kwargs.get('post_spacing', 5)
        type_ = kwargs.get('type_', "NA")
        contractor = kwargs.get('contractor', False)

        # FURTHER INPUT VALUES BASED ON METHOD INPUTS
        non_electric_wires = no_wires - electric_wires

        # SUMMING PARTS OF FENCE TO DETERMINE FENCE COST

        if battons == True:
            battons_pm_adjusted = (post_spacing - batton_spacing) / post_spacing
            battons_cost = (self.FI.cost_dict['Fence Post - Batton'][self.FI.ucl] / batton_spacing) * length + \
                            self.FI.cost_dict['Fence Post - Batton'][self.FI.bcl]
        else:
            battons_pm_adjusted = 0
            battons_cost = 0

        if contractor == True:
            contractor_cost = self.FI.cost_dict['Contractor - ' + type_][self.FI.ucl] * length + \
                              self.FI.cost_dict['Contractor - ' + type_][self.FI.bcl]
        else:
            contractor_cost = 0

        no_staples_pm = (1 / post_spacing) * non_electric_wires + (1 / battons_pm_adjusted) * non_electric_wires

        staples_cost            = length * no_staples_pm * self.FI.cost_dict['Staples'][self.FI.ucl] + \
                                  self.FI.cost_dict['Staples'][self.FI.bcl]

        wire_cost               = length * no_wires * self.FI.cost_dict['Wire - Standard'][self.FI.ucl] + \
                                  self.FI.cost_dict['Wire - Standard'][self.FI.bcl]

        post_insulators_cost    = length * (1 / post_spacing) * electric_wires * \
                                  self.FI.cost_dict['Insulators - Post'][self.FI.ucl] + \
                                  self.FI.cost_dict['Insulators - Post'][self.FI.bcl]


        batton_insulators_cost  = length * (1 / battons_pm_adjusted) * electric_wires * \
                                  self.FI.cost_dict['Insulators - Batton'][self.FI.ucl] + \
                                  self.FI.cost_dict['Insulators - Batton'][self.FI.bcl]

        post_cost               = length * (1/post_spacing) * \
                                  self.FI.cost_dict['Fence Post - '+ post_type][self.FI.ucl] + \
                                  self.FI.cost_dict['Fence Post - '+ post_type][self.FI.bcl]

        no_gates = self.gate_fence_length / self.FI.pad_width
        no_gates = no_gates + no_gates / 2  # this accounts for internal gates.
        gate_type = kwargs.get('gate_type', 'Gate - 12ft SS')
        gate_cost = self.FI.cost_dict[gate_type][self.FI.ucl] * no_gates + \
                    self.FI.cost_dict[gate_type][self.FI.bcl]

        # note that this probably oversetimates the amount of gates.

        no_gate_posts = no_gates
        no_strainers = (self.FI.boundary_length / self.FI.pad_width + 1) * (
                    1 + self.FI.boundary_width / self.FI.pad_length)
        no_corners = (self.FI.boundary_length / self.FI.pad_width + 1) * (
                    1 + self.FI.boundary_width / self.FI.pad_length)

        # TODO decide what to use strainers or posts. Also need to factor post hanging the fence posts

        corner_post_cost = self.FI.cost_dict['Fence Post - Corner'][self.FI.ucl] * no_corners + \
                           self.FI.cost_dict['Fence Post - Corner'][self.FI.bcl]

        strainer_post_cost = self.FI.cost_dict['Fence Post - Strainer'][self.FI.ucl] * no_strainers + \
                             self.FI.cost_dict['Fence Post - Strainer'][self.FI.bcl]

        gate_post_cost = self.FI.cost_dict['Fence Post - Gate Post'][self.FI.ucl] * no_gate_posts + \
                         self.FI.cost_dict['Fence Post - Gate Post'][self.FI.bcl]

        fence_cost =  {'Staples'               : length * staples_cost,
                       'Wire'                  : length * wire_cost,
                       'Insulators - Post'     : length * post_insulators_cost,
                       'Insulators - Battons'  : length * batton_insulators_cost,
                       'Post - '+post_type     : length * post_cost,
                       'Post - Batton'         : length * battons_cost,
                       'Contractor - ' + type_ : contractor_cost,
                       'Gates'                 : gate_cost,
                       'Post - Corner'         : corner_post_cost,
                       'Post - Strainer'       : strainer_post_cost,
                       'Post - Gate'           : gate_post_cost}

        total_fence_cost = sum(fence_cost.values())

        return total_fence_cost


class MilkingShed:

    def __init__(self, fi_object, **kwargs):

        self.FI = fi_object
        self.mm_type = kwargs.get('mm_type','rotary') # options are: rotary, hb, swing_over
        self.bails = kwargs.get('bails', 'NA')
        self.dealer_distance = kwargs.get('dealer_distance', 10) # distsnce from shed inkm

    def base_mm_calc(self):

        if self.mm_type == 'rotary':

            cost = self.FI.cost_dict['Basic Milking Machine - ' + str(self.bails) +'B - Rotary'][self.FI.ucl] * 1 + \
                   self.FI.cost_dict['Basic Milking Machine - ' + str(self.bails) +'B - Rotary'][self.FI.bcl]

        else:

            cost = self.FI.cost_dict['Basic Milking Machine - ' + str(self.bails) +'AS - HB'][self.FI.ucl] * self.bails + \
                   self.FI.cost_dict['Basic Milking Machine - ' + str(self.bails) +'AS - HB'][self.FI.bcl]

        return cost

    def base_platform_calc(self):

        if self.mm_type == 'rotary':

            cost = self.FI.cost_dict['Basic Rotary Platform - ' + str(self.bails) +'B - Rotary'][self.FI.ucl] * self.bails + \
                   self.FI.cost_dict['Basic Rotary Platform - ' + str(self.bails) +'B - Rotary'][self.FI.bcl]

        else:

            cost = 0

        return cost

    def mm_cost(self, **kwargs):

        # set upgrades:

        wash_gland = kwargs.get('wash_gland', False)
        ecrs = kwargs.get('cup_removers', True)
        teat_spray = kwargs.get('teat_spray', True)
        mastitis_detection = kwargs.get('mastitis_detection', False)
        milk_yeild_meters = kwargs.get('yield_meters', False)
        fat_protein_meters = kwargs.get('fat/protein_meters', False)
        mm_auto_wash = kwargs.get('auto_wash_machine', True)
        silo_auto_wash = kwargs.get('silo_auto_wash', False)
        no_silos = kwargs.get('no_silos', 2)

        if wash_gland:

            cost_wash_gland = self.FI.cost_dict['3 Port Wash Gland'][self.FI.ucl] * 1 + \
                              self.FI.cost_dict['3 Port Wash Gland'][self.FI.bcl]
        else:

            cost_wash_gland = 0

        if ecrs:

            cost_ecrs = self.FI.cost_dict['Cup Removers'][self.FI.ucl] * self.bails + \
                    self.FI.cost_dict['Cup Removers'][self.FI.bcl]

        else:

            cost_ecrs = 0

        if teat_spray:

            cost_teat_spray = self.FI.cost_dict['In Bail Teat Spray'][self.FI.ucl] * self.bails + \
                              self.FI.cost_dict['In Bail Teat Spray'][self.FI.bcl]

        else:

            cost_teat_spray = 0

        if mastitis_detection:

            cost_mastitis_detection = self.FI.cost_dict['Mastitis Detection'][self.FI.ucl] * self.bails + \
                                      self.FI.cost_dict['Mastitis Detection'][self.FI.bcl]

        else:

            cost_mastitis_detection = 0

        if milk_yeild_meters:

             cost_milk_yield_indicators = self.FI.cost_dict['Milk Yield Indicators'][self.FI.ucl] * self.bails + \
                                         self.FI.cost_dict['Milk Yield Indicators'][self.FI.bcl]

        else:

            cost_milk_yield_indicators = 0

        if fat_protein_meters:

            cost_milk_fat_protein_indicators = self.FI.cost_dict['Milk Fat / Protein Indicators'][self.FI.ucl] * self.bails + \
                                               self.FI.cost_dict['Milk Fat / Protein Indicators'][self.FI.bcl]

        else:

            cost_milk_fat_protein_indicators = 0

        if mm_auto_wash:

            cost_mm_auto_wash = self.FI.cost_dict['Wash Automation: Machine'][self.FI.ucl] * 1 + \
                                self.FI.cost_dict['Wash Automation: Machine'][self.FI.bcl]

        else:

            cost_mm_auto_wash = 0

        if silo_auto_wash is False:

            cost_silo_auto_wash = 0

        else:

            cost_silo_auto_wash = self.FI.cost_dict['Wash Automation: Silo'][self.FI.ucl] * no_silos + \
                                  self.FI.cost_dict['Wash Automation: Silo'][self.FI.bcl]

        mm_cost = {'Milking Machine'       : MilkingShed.base_mm_calc(self),
                   'Wash Gland'            : cost_wash_gland,
                   'Cup Removers'          : cost_ecrs,
                   'Teat Spray'            : cost_teat_spray,
                   'Mastitis Detection'    : cost_mastitis_detection ,
                   'Milk Yield Meters'     : cost_milk_yield_indicators,
                   'Milk Fat/Protein Meters' : cost_milk_fat_protein_indicators,
                   'Machine Auto Wash'     : cost_mm_auto_wash,
                   'Silo Auto Wash'        : cost_silo_auto_wash}

        total_mm_cost = sum(mm_cost.values())

        return total_mm_cost

    def platform_cost(self, **kwargs):

        extra_engineering = kwargs.get('extra_engineering', False)

        if extra_engineering:

            cost_extra_engineering = self.FI.cost_dict['Additional Engineering'][self.FI.ucl] * 1 + \
                                     self.FI.cost_dict['Additional Engineering'][self.FI.bcl]

        else:

            cost_extra_engineering = 0

        pf_cost = {'Platform Cost': MilkingShed.base_platform_calc(self),
                   'Extra Engineering': cost_extra_engineering}

        total_pf_cost = sum(pf_cost.values())

        return total_pf_cost

    def shed_water_cost (self, **kwargs):

        cost_shed_water = self.FI.cost_dict['Shed Water Pumps and Plumbing - ' + self.bails + 'B'][self.FI.ucl] * 1 + \
                          self.FI.cost_dict['Shed Water Pumps and Plumbing - ' + self.bails + 'B'][
                              'Initial $/ Farm (Low)']

        nose_spray = kwargs.get('nose_spray', False)
        farm_pump_bypass = kwargs.get('farm_pump_bypass', True)
        hwc_auto_fill = kwargs.get('hwc_auto_fill', True)
        extra_tank = kwargs.get('plastic_tank', False)
        tank_size = kwargs.get('tank_size', True)
        calf_milk_pump = kwargs.get('calf_milk_pump', True)
        washddown_vsd = kwargs.get('washdown_vsd', False)
        upgrade_washdown_pump = kwargs.get('washdown_and_VSD_pump_upgrade', False)
        upgrade_cooler_pump = kwargs.get('cooler_pump_upgrade', False)

        if nose_spray:
            cns = self.FI.cost_dict['Nose Spray System'][self.FI.ucl] * 1 + \
                  self.FI.cost_dict['Nose Spray System'][self.FI.bcl]
        else:
            cns = 0
        if farm_pump_bypass:
            cfpb = self.FI.cost_dict['Farm Pump Bypass'][self.FI.ucl] * 1 + \
                   self.FI.cost_dict['Farm Pump Bypass'][self.FI.bcl]
        else:
            cfpb = 0
        if hwc_auto_fill:
            chwcaf = self.FI.cost_dict['Hot Water Cylinders - Autofill'][self.FI.ucl] * 1 + \
                     self.FI.cost_dict['Hot Water Cylinders - Autofill'][self.FI.bcl]
        else:
            chwcaf = 0
        if extra_tank:
            cet = self.FI.cost_dict['Plastic Water Tank - ' + tank_size + 'L with plumbing'][self.FI.ucl] * 1 + \
                  self.FI.cost_dict['Plastic Water Tank - ' + tank_size + 'L with plumbing'][self.FI.bcl]
        else:
            cet = 0
        if calf_milk_pump:
            ccmp = self.FI.cost_dict['Calf Milk - Pumping and Reticulation'][self.FI.ucl] * 1 + \
                   self.FI.cost_dict['Calf Milk - Pumping and Reticulation'][self.FI.bcl]
        else:
            ccmp = 0
        if washddown_vsd:
            cwvsd = self.FI.cost_dict['VSD - Washdown Pump'][self.FI.ucl] * 1 + \
                    self.FI.cost_dict['VSD - Washdown Pump'][self.FI.bcl]
        else:
            cwvsd = 0
        if upgrade_washdown_pump:
            cwdpu = self.FI.cost_dict['Pump - Washdown and VSD Upgrade'][self.FI.ucl] * 1 + \
                    self.FI.cost_dict['Pump - Washdown and VSD Upgrade'][self.FI.bcl]
        else:
            cwdpu = 0
        if upgrade_cooler_pump:
            ccpu = self.FI.cost_dict['Pump - Cooler Upgrade'][self.FI.ucl] * 1 + \
                   self.FI.cost_dict['Pump - Cooler Upgrade'][self.FI.bcl]
        else:
            ccpu = 0

        shed_water_cost = {'Shed Water - Base' : cost_shed_water,
                           'Nose Spray System' : cns,
                           'Farm Pump Bypass' : cfpb,
                           'Hot Water Cylinders Auto Fill' : chwcaf,
                           'Water Tank - ' + tank_size + 'L' : cet,
                           'Calf Milk Pump' : ccmp,
                           'Washdwon - VSD' : cwvsd,
                           'Washdown Pump Upgrade': cwdpu,
                           'Cooler Pump Upgrade': ccpu}

        total_shed_water_cost = sum(shed_water_cost.values())

        return total_shed_water_cost

    def shed_electrical_cost (self, **kwargs):
        pass

    def shed_building_cost (self, **kwargs):
        pass

    def shed_engineering_costs (self, **kwargs):
        pass


class Effluent:

    def __init__(self, fi_object, **kwargs):

        # INHERITING GENERAL FARM INPUTS #
        self.FI = fi_object

        # EFFLUENT INPUTS #
        self.eff_area_current = kwargs.get('current_effluent_area', 0)
        self.eff_area_extra = kwargs.get('additional_effluent_area', 0)
        self.regional_council = kwargs.get('regional_council', 'Environment Waikato') # make a dropdown
        self.effluent_type = kwargs.get('effleunt_type', 'standard')
        self.eff_block_dis = kwargs.get('distance_pond_to_efflunet_block', 0)
        self.eff_per_cow_per_day = 70
        self.eff_area = kwargs.get('effleunt_area','NA')
        self.min_eff_area = Effluent.eff_area_calc(self)
        self.eff_area_low_risk = kwargs.get('eff_area_low_risk', 0)
        self.min_eff_area_high_risk = kwargs.get('eff_area_high_risk', self.min_eff_area - self.eff_area_low_risk)
        self.remaining_eff_area = self.eff_area - self.eff_area_low_risk - self.min_eff_area_high_risk

        # self.effluent_pump
        self.Deficit = type('deficit_table', (self.Base,), {'__tablename__': 'cliflo_rainfall_station_info_test'})

    @staticmethod
    def effleunt_catchemnts(**kwargs):

        # TODO get standard areas from table I will make later that references different sheds
        # TODO make stormwater beabel to be divereted under more options not just when dry
        yard_area = kwargs.get('yard_area', 100)
        shed_roof_area = kwargs.get('shed_roof_area', 100)
        non_roof_swd = kwargs.get('non_roof_swd', True)
        shed_roof_swd = kwargs.get('roof_swd', True)
        other_area = kwargs.get('other_area', 100)

        catchment_dict = {'yard':  {'area': yard_area,
                                    'swd': non_roof_swd},
                          'shed':  {'area': shed_roof_area,
                                    'swd': shed_roof_swd},
                          'other': {'area': other_area,
                                    'swd': False}}

        return catchment_dict

    @staticmethod
    def effluent_wash_water_volume(**kwargs):

        use_default = kwargs.get('use_default', False)
        if use_default:
            volume = 70

        #TODO add in user inputs for plant and vat wash etc as per DESC guides

        return volume

    def effluent_irrigation_costs(self, **kwargs):
        """
        TODO make this method more accurate.
        :param self:
        :param kwargs:
        :return:
        """

        #### KWARGS ####

        through_pivots = kwargs.get('through_pivots', True)
        irrigator_type = kwargs.get('irrigator_type', 'Travelling Rain Gun') #options are 'rain gun', 'pods'
        irrigator_hose_length = kwargs.get('irrigator_hose_length', 150)
        no_pads = kwargs.get('no_eff_pads', self.eff_area / (self.FI.pad_size_proposed * 0.9))
        arterial_size = kwargs.get('arterial_size', 90) # TODO check this line size
        length_arterials = kwargs.get('length_arterials',no_pads * self.FI.pad_width)
        no_pipe_ends = kwargs.get('no_pipe_ends',2)
        no_2_way_hydrants = kwargs.get('no_2_way_hydrants', 0)
        no_3_way_hydrants = kwargs.get('no_3_way_hydrants', no_pads - no_pipe_ends)
        other_pipe_length = kwargs.get('length_other_pipe',0)
        other_pipe_size = kwargs.get('length_other_pipe',45)
        other_pipe_length_2 = kwargs.get('length_other_pipe_2', 0)
        other_pipe_size_2 = kwargs.get('length_other_pipe_2', 45)
        max_elevation = kwargs.get('max_elevation',0) # height in m
        sump_contengency_days=kwargs.get('sump_contingency_days',3)

        #### CALCS FROM KWARGS ####

        eff_pipe_length = length_arterials + other_pipe_length + other_pipe_length_2

        #### COST ESTIMATE OF EFFLUENT PIPELINE SYSTEM ####

        #Arterial Cost
        ca = self.FI.cost_dict['Pipe - ' + arterial_size + 'mm  - Alk'][self.FI.ucl] * length_arterials + \
             self.FI.cost_dict['Pipe - ' + arterial_size + 'mm  - Alk'][self.FI.bcl]
        #Other Pipe Cost
        cop = self.FI.cost_dict['Pipe - ' + other_pipe_size + 'mm  - Alk'][self.FI.ucl] * other_pipe_length + \
              self.FI.cost_dict['Pipe - ' + other_pipe_size + 'mm  - Alk'][self.FI.bcl]
        #Other Pipe 2 Cost
        cop_2 = self.FI.cost_dict['Pipe - ' + other_pipe_size_2 + 'mm  - Alk'][self.FI.ucl] * other_pipe_length_2 + \
                self.FI.cost_dict['Pipe - ' + other_pipe_size_2 + 'mm  - Alk'][self.FI.bcl]
        #Trenching Cost
        ct = self.FI.cost_dict['Effluent Line - Trenching'][self.FI.ucl] * eff_pipe_length + \
             self.FI.cost_dict['Effluent Line - Trenching'][self.FI.bcl]
        #Pipe Install Costs
        cep_install = self.FI.cost_dict['Effluent Line - '][[self.FI.ucl]] * eff_pipe_length + \
                      self.FI.cost_dict['Effluent Line - Trenching'][self.FI.bcl]
        #2 Way Hydrants Cost
        ch_two = self.FI.cost_dict['Hydrant - 2W'][[self.FI.ucl]] * no_2_way_hydrants + \
                 self.FI.cost_dict['Hydrant - 2W'][self.FI.bcl] + \
                 self.FI.cost_dict['Hydrant - 2W - Install'][[self.FI.ucl]] * no_2_way_hydrants + \
                 self.FI.cost_dict['Hydrant - 2W - Install'][self.FI.bcl]
        #3 Way Hydrants Cost
        ch_three = self.FI.cost_dict['Hydrant - 3W'][self.FI.ucl] * no_3_way_hydrants + \
                   self.FI.cost_dict['Hydrant - 3W'][self.FI.bcl] + \
                   self.FI.cost_dict['Hydrant - 3W - Install'][self.FI.ucl] * no_3_way_hydrants + \
                   self.FI.cost_dict['Hydrant - 3W - Install'][self.FI.bcl]
        #3 Way Hydrants Cost
        ch_eol = self.FI.cost_dict['Hydrant - EOL'][self.FI.ucl] * no_pipe_ends + \
                 self.FI.cost_dict['Hydrant - EOL'][self.FI.bcl] + \
                 self.FI.cost_dict['Hydrant - EOL - Install'][self.FI.ucl] * no_pipe_ends + \
                 self.FI.cost_dict['Hydrant - EOL - Install'][self.FI.bcl]

        #### COST ESTIMATE OF EFFLUENT PUMPING SYSTEM ####

        # TODO Take pipe lengths and create a headloss calc for selecting the right pump probablay make into a method

        #3 Pumping Sustem

        pump_size = kwargs.get('pump_size',18.5) # in kw
        pump_type = kwargs.get('pump_type', 'Close Couple')

        # pumping system cost
        c_ep = self.FI.cost_dict['Pump - Effluent - ' + pump_size + 'kw - ' + pump_type][self.FI.ucl] * 1 + \
              self.FI.cost_dict['Pump - Effluent - ' + pump_size + 'kw - ' + pump_type][self.FI.bcl] + \
              self.FI.cost_dict['Pump - Effluent - Install'][self.FI.ucl] * 1 + \
              self.FI.cost_dict['Pump - Effluent - Install'][self.FI.bcl] + \
              self.FI.cost_dict['Pump - Effluent - Stand'][self.FI.ucl] * 1 + \
              self.FI.cost_dict['Pump - Effluent - Stand'][self.FI.bcl] + \
              self.FI.cost_dict['Pump - Effluent - Hose'][self.FI.ucl] * 1 + \
              self.FI.cost_dict['Pump - Effluent - Hose'][self.FI.bcl]

        # cost of Irrigator
        c_irr = self.FI.cost_dict['Irrigator - ' + irrigator_type][self.FI.ucl] * 1 + \
                self.FI.cost_dict['Irrigator - ' + irrigator_type][self.FI.bcl]        #

    def effleunt_electrical_install_costs(self, **kwargs):
        # TODO electrical install costs.
        pass

    def eff_area_calc(self):

        """
        TODO complete this function
        checks that the proposed area mathes the minimium area for that region.
        :return:
        """

        # effleunt limits in kg/ha/year
        if self.FI.regioanl_council == 'Environment Waikato':
            effluent_limits = {'N' : 150,
                               'P' : 999999999,
                               'K' : 999999999,
                               'S' : 999999999}
        else:
            pass
        # Effluent Concentrations in g/m3
        if self.FI.effluent_type == 'standard':
            nutrient_conc = { 'N' : 424,
                              'P' : 49.9,
                              'K' : 399,
                              'S' : 32}
        else:
            pass

        #TODO this is wrong. IT may be 70L per cow per day in total!! Actually I thing 70L over 24hrs is good.
        effluent_vol = (self.eff_per_cow_per_day * self.FI.stock_milking * (self.FI.milking_time/24) *
                             self.FI.lactation_days * (1/1000) )

        min_N_area = effluent_vol * nutrient_conc['N'] *(1/1000) / effluent_limits['N']
        min_P_area = effluent_vol * nutrient_conc['P'] *(1/1000) / effluent_limits['P']
        min_K_area = effluent_vol * nutrient_conc['K'] *(1/1000) / effluent_limits['K']
        min_S_area = effluent_vol * nutrient_conc['S'] *(1/1000) / effluent_limits['S']

        min_area = max(min_K_area,min_N_area,min_P_area,min_S_area)

        eff_area = max(min_area, self.eff_area_current + self.eff_area_extra)

        return eff_area

    def effleunt_storage_costs(self, **kwargs):
        pass

    def effluent_volume(self):
        """
        Query's the SQL database to get the stations rainfall for each day over 30 years and
        calculates how much daily effluent inters the effluent system. It also adds the volume of the
        cows effluent and the volume of the wash area.
        :param kwargs:
        :return:
        """

        Stations = type('station_data', (self.FI.Base,), {'__tablename__': 'cliflo_rainfall_station_info_table_test2'})
        Deficit = type('deficit_table', (self.FI.Base,), {'__tablename__': 'cliflo_rainfall_daily_test2'})
        station_location_list = [({'lat': float(d.lat), 'lon': float(d.long), 'station': int(d.stations)})
                                 for d in self.FI.session.query(Stations).all()]

        farm_location = {'lat': self.FI.lat, 'lon': self.Fi.long}

        distance = [(vals['station'], geopy.distance.distance((farm_location['lat'], farm_location['lon']),
                                                              (vals['lat'], vals['lon'])).km)
                    for vals in station_location_list]

        for val in range(0, 10):
            closest_tuple = (min(distance, key=lambda t: t[1]))
            closest_station = closest_tuple[0]
            query_total = self.FI.session.query(Deficit).filter(Deficit.station == str(closest_station))
            query_est = self.FI.session.query(Deficit).filter(Deficit.station == str(closest_station),
                                                                   Deficit.deficit_mm_estimated == '1')
            if (query_est.count() / query_total.count()) < 0.05:
                break
            else:
                # if tuple has too many errors (over 5 percent) recalcualte distance tuple and exclude the closest one)
                distance = [i for i in distance if i[0] != closest_station]

        # create database for volumes of effluent generated over 30 years

        station_info = self.session.query(Deficit).filter(Deficit.station == str(closest_station))

        eff_vol_df = pd.Dataframe(columns=['year', 'month', 'day', 'eff_vol_rainfall', 'eff_vol_cow', 'eff_vol_wash'],
                                  index=np.arrange(0, station_info.count()))

        # TODO make the efflenect scale up (i.e at this stage all cows are dried on at the same time but this would
        # usually take a month or two over calving and a month to dry off.

        start_milk_date = datetime.datetime.strptime(str(self.FI.milk_start_month) + str(self.FI.milk_start_month), "%m/%d/")
        end__milk_date = start_milk_date + datetime.timedelta(days=self.FI.lactation_days)

        catch_dict = Effluent.effluent_catchments()

        for d, index in enumerate(station_info, 0):

            if start_milk_date<= datetime.datetime.strptime(str(d.month) + str(d.day), "%m/%d/") <= end__milk_date:
                eff_vol_cow = self.eff_per_cow_per_day * 0.001
                eff_vol_wash = Effluent.effluent_wash_water_volume(use_default=True) * 0.001
                if catch_dict['shed']['swd']:
                    shed_area = 0
                else:
                    shed_area = catch_dict['shed']['area']
                captured_area = catch_dict['yard']['area'] + shed_area + catch_dict['other']['area']
            else:
                eff_vol_cow = 0
                eff_vol_wash = 0
                if catch_dict['yard']['swd']:
                    yard_area = 0
                else:
                    yard_area = catch_dict['yard']['area']
                if catch_dict['shed']['swd']:
                    shed_area = 0
                else:
                    shed_area = catch_dict['shed']['area']
                captured_area = yard_area + shed_area + catch_dict['other']['area']

            eff_vol_rain = d.ammoount_mm/100 * captured_area  # this is in m3

            eff_vol_df.loc[index] = [d.year, d.month, d.day, eff_vol_rain, eff_vol_cow, eff_vol_wash, d.deficit_mm]

        return eff_vol_df

    def effluent_storage_calc(self, **kwargs):


        eff_vol_df = Effluent.effluent_volume(self)

        w_s_depth         = kwargs.get('winter_spring_depth', 3)
        s_a_depth         = kwargs.get('spring_autumn_depth', 3)
        w_s_vol           = kwargs.get('winter_spring_vol', 3)      # m3 / hr
        s_a_vol           = kwargs.get('spring_autumn_vol', 3)      # m3 / hr
        w_s_hrs           = kwargs.get('winter_spring_hrs', 3)      # hours per day
        s_a_hrs           = kwargs.get('spring_autumn_hrs', 3)      # hours per day
        irrigate_all_year = kwargs.get('irrigate_all_year', True)
        irrigate_start_month = kwargs.get('irrigate_start_date_month', 9)
        irrigate_start_day   = kwargs.get('irrigate_start_date_month', 1)
        irrigate_end_month   = kwargs.get('irrigate_start_date_month', 9)
        irrigate_end_day     = kwargs.get('irrigate_start_date_month', 1)

        w_s_vol_per_day = w_s_vol * w_s_hrs
        s_a_vol_per_day = s_a_vol * s_a_hrs

        start_irrigation_date = datetime.datetime.strptime(str(self.FI.milk_start_month)
                                                           + str(self.FI.milk_start_month), "%m/%d/")
        end__irrgation_date = datetime.datetime.strptime(str(self.FI.milk_start_month)
                                                         + str(self.FI.milk_start_month), "%m/%d/")

        for index, row in eff_vol_df.iterrows():
            pass

        # Keep track of how much you can apply frm effluent over a given time
        # every day some volume is going to be put over an area which will then be take out of use if it is
        # above the depth amount.
        # this is going to take some time to think through.

    def eff_sump_costs(self):

        vol_per_milkng = ((self.eff_per_cow_per_day * (self.FI.milking_time/24) * self.FI.stock_milking / 1000) +
                            self.FI.mm_wash_water + ( self.FI.stock_milking*self.FI.washdown_water_per_cow / 1000 ))

        sump_sizes = [50,100,150,200,250]

        for size in sump_sizes:

            if size < vol_per_milkng:
                pass
            else:
                #cost of sump
                csump = self.FI.cost_dict['Sump - ' + size + 'm3 storage'] * 1 + \
                        self.FI.cost_dict['Sump - ' + size + 'm3 storage']['Initial $/ Farm (Low)']
                break

        #TODO change size of stirre based on sump
        #TODO add install cost of sump if necessary
        #TODO size pumps based on sump size
        #TODO implemnt NIWA hirds option to size pump if required

        c_sitrrer = self.FI.cost_dict['Stirrer - Sump - Large'][self.FI.ucl] * 1 + \
                    self.FI.cost_dict['Stirrer - Sump - Large'][self.FI.bcl]

        c_sump_pump = self.FI.cost_dict['Pump - Effluent - 15kw - Sump'][self.FI.ucl] * 1 + \
                      self.FI.cost_dict['Pump - Effluent - 15kw - Sump'][self.FI.bcl]


class LandCover:

    def __init__(self, fi_object):
        self.FI = fi_object
        pass

    def cover_removal(self):
        pass

    def horticulture(self, **kwargs):

        hort_type = kwargs.get('type', "NA")

        #lets user set trees per hectare otherwise uses default from dictionary {type:[low,high]}
        trees_per_ha_dict = {'Avacado':[400, 400, 0],
                             'Apples - Jazz': [800, 850,900],
                             }

        trees_per_ha = kwargs.get('trees_per_ha', trees_per_ha_dict[hort_type][0])
        trellis = kwargs.get('trellis', False)
        land_preparation = kwargs.get('land_preperation', False)
        trees_per_farm = self.FI.land_area * trees_per_ha

        tree_cost = self.FI.cost_dict[hort_type + ' - Trees'][self.FI.ucl] * self.FI.land_area + \
                    self.FI.cost_dict[hort_type + ' - Trees'][self.FI.bcl]
        labour_cost = self.FI.cost_dict[hort_type + ' - Tree Planting'][self.FI.ucl] * self.FI.land_area + \
                      self.FI.cost_dict[hort_type + ' - Tree Palnting'][self.FI.bcl]

        pass

    def forestry(self, **kwargs):

        forest_type = kwargs.get('forest_type', "NA")

        tree_cost = self.FI.cost_dict[forest_type + ' - Trees'][self.FI.ucl] * self.FI.land_area + \
                    self.FI.cost_dict[forest_type + ' - Trees'][self.FI.bcl]
        labour_cost = self.FI.cost_dict[forest_type + ' - Planting'][self.FI.ucl] * self.FI.land_area + \
                      self.FI.cost_dict[forest_type + ' - Planting'][self.FI.bcl]

        total_cost = tree_cost + labour_cost

        return total_cost





        pass

    def apples(self):
        pass

    def blueberries(self):
        pass

    def kiwifruit(self):
        pass

    def hortculture(self):
        pass

    def pasture(self):
        pass

    def native_trees(self):
        pass

    def grapes(self):
        pass

    def hops(self):
        pass


class Infrastructure:

    def __init__(self):
        pass

    def races(self):
        pass

    def truck_race(self):
        pass

    def calf_shed(self):
        pass

    def implement_shed(self):
        pass

    def packing_house(self):
        pass

    def accomodation(self):
        pass


class Shelter:

    def __init__(self):
        pass

    def shelter_belt(self):
        pass


class FarmWater:
    pass
    # FARM WATER, BORE WATER PUMP, DOSEATRON SYSTEM, FARM WATER VSD


class Irrigation:
    pass


class WorkingCapital:
    pass


# Iniital inputs required to get the farm working properly will  move this to land cover method
class NutrientInputs:
    pass


class EnvironmentInputs:

    def __init__(self, **kwargs):
    pass


####################
# USEFUL FUNCTIONS #
####################

































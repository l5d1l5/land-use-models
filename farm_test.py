import Model_Parameters as MP
# import CapexCalc as CC
# import OpexCala as OC
# import ClimateData as CD


def main():

    test_farm = MP.FarmInputs(current_land_use='Dairy - Cow',
                              proposed_land_use='Dairy - Sheep',
                              current_paddock_size = 2,
                              proposed_paddock_size = 2,
                              land_area = 100,
                              pad_length_to_width = 2,
                              farm_length_to_width = 2,
                              db_name='sandpit',
                              db_user_name='postgres',
                              db_user_password='&MaM!r0postgres&',
                              main_table='mamirodata2')

    fence_setup = MP.Fences(test_farm)

    ms_setup = MP.MilkingShed(test_farm,
                                 mm_type='rotary',
                                 bails = 54,
                                 dealer_distance=10)


    if __name__ == '__main__':
        mm_test = ms_setup.mm_cost( wash_gland = False,
                                    cup_removers = True,
                                    teat_spray = True,
                                    mastitis_detection = False,
                                    yield_meters=False,
                                    fat_protein_meters=False,
                                    auto_wash_machine=True,
                                    silo_auto_wash=False,
                                    no_silos=2)

        pf_test = ms_setup.platform_cost(extra_engineering=False)

        fence_test = fence_setup.fence_cost(type='Post & Batton - 2 Wires Electric',
                                            length=100)

        print('fence test: ', fence_test)
        print('milking machine test: ', mm_test)
        print('platform test: ', pf_test)

if __name__ == "__main__":
    main()


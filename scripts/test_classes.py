import urllib.request
import urllib.parse

import pandas as pd
from selenium import webdriver
import time
import pyautogui



def main ():

    switch = 'off'
    path = 'C:/Users/risid/Google Drive/Python/land-use-models/data/niwa_rainfall/'
    stations = pd.read_excel(path+'Station_Data.xlsx')

    for station_id in stations['AgentNumber']:
        if station_id == 5396:
            switch = 'on'
        else:
            pass

        if switch == 'on':

            for year in list(range(1988, 2018)):
                try:

                    ###############################
                    # Logging into Cliflo Website #
                    ###############################

                    driver = webdriver.Chrome('C:/webdrivers/chromedriver.exe')
                    driver.set_page_load_timeout(100)
                    driver.get('https://cliflo.niwa.co.nz/')
                    driver.find_element_by_name('cusername').send_keys('mamirodata')
                    driver.find_element_by_name('cpwd').send_keys('&MaM!r0niwa&')
                    time.sleep(1)
                    driver.find_element_by_name('submit').click()
                    time.sleep(1)
                    driver.find_element_by_name('sub_refresh').click()
                    time.sleep(2)
                    #store current window handle
                    main_window_handle = driver.current_window_handle


                    ######################################
                    # Selecting Weather Stations we want #
                    ######################################

                    driver.find_element_by_name('datatype2').click()
                    driver.switch_to.window(driver.window_handles[-1])
                    time.sleep(1)
                    driver.find_element_by_link_text("Daily and Hourly Observations").click()
                    time.sleep(1)
                    driver.find_element_by_link_text("Precipitation").click()
                    time.sleep(1)
                    driver.find_element_by_link_text("Rain (fixed periods)").click()
                    time.sleep(1)
                    driver.switch_to.window(main_window_handle)
                    time.sleep(1)
                    driver.find_element_by_name('agent').click()
                    time.sleep(1)
                    driver.switch_to.window(driver.window_handles[-1])
                    time.sleep(1)
                    driver.find_element_by_css_selector("input[type='radio'][value='ag']").click()
                    time.sleep(1)
                    driver.find_element_by_name('cAgent').send_keys(station_id)
                    time.sleep(1)
                    driver.find_element_by_name('Submit').click()
                    time.sleep(1)
                    driver.switch_to.window(driver.window_handles[-1])
                    time.sleep(1)
                    driver.find_element_by_xpath("//input[@name='cstn' and @value='" + str(station_id) + "']").click()
                    time.sleep(1)
                    driver.find_element_by_xpath("//input[@name='Submit' and @value='Replace Selected Stations']").click()
                    time.sleep(1)
                    driver.switch_to.window(main_window_handle)

                    driver.find_element_by_name('date1_1').clear()
                    driver.find_element_by_name('date1_2').clear()
                    driver.find_element_by_name('date1_3').clear()
                    driver.find_element_by_name('date1_4').clear()

                    time.sleep(1)

                    driver.find_element_by_name('date2_1').clear()
                    driver.find_element_by_name('date2_2').clear()
                    driver.find_element_by_name('date2_3').clear()
                    driver.find_element_by_name('date2_4').clear()

                    driver.find_element_by_name('date1_1').send_keys(year)
                    driver.find_element_by_name('date1_2').send_keys(1)
                    driver.find_element_by_name('date1_3').send_keys(1)
                    driver.find_element_by_name('date1_4').send_keys(00)

                    time.sleep(1)

                    driver.find_element_by_name('date2_1').send_keys(year+1)
                    driver.find_element_by_name('date2_2').send_keys(1)
                    driver.find_element_by_name('date2_3').send_keys(1)
                    driver.find_element_by_name('date2_4').send_keys(00)

                    time.sleep(1)

                    driver.find_element_by_name('submit_sq').click()
                    time.sleep(3)

                    # open 'Save as...' to save html and assets
                    pyautogui.hotkey('ctrl', 's')

                    time.sleep(1)
                    string = '\\Users\\risid\\Google Drive\\Python\\land-use-models\\data\\niwa_rainfall\\station ' \
                             + str(station_id)+' - daily_precipitation (' + str(year) + ' - ' +str(year+1) + ')'
                    pyautogui.typewrite(string)
                    time.sleep(1)
                    pyautogui.hotkey('enter')

                    time.sleep(1)
                    driver.quit()

                    print('station: '+station_id + ', year: '+ year)

                except:

                    print('error: ' + str(station_id))

                    pass
        else:

            print('skipping station: ' + str(station_id))




if __name__ == "__main__":
    main()







# path = 'C:/Users/risid/Google Drive/Python/land-use-models/data/niwa_rainfall/'
# fn1 = 'Data Output.html'
# fn2 = 'Station Listing Using Datatypes.html'
#
# data = pd.read_html(path+fn2)
#
# station_df = data[1]
#
# station_df.columns = station_df.iloc[0]
# station_df.drop(0, inplace=True)
# station_df.reset_index(inplace=True)
# station_df['start_day'], station_df['start_month'], station_df['start_year'] = station_df['Start Date'].astype(str).str.split('-', 2).str
# station_df['end_day'], station_df['end_month'], station_df['end_year'] = station_df['End Date'].astype(str).str.split('-', 2).str
# cols = ['start_day','start_year','end_day','end_year']
# station_df[cols] = station_df[cols].apply(pd.to_numeric, errors='coerce')
# station_df_filter = station_df.loc[(station_df['end_year'] > 2018) & (station_df['start_year'] < 2018-30) & (station_df['PercentComplete'] == '100')]



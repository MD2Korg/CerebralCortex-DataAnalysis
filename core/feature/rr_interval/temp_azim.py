from cerebralcortex.cerebralcortex import CerebralCortex

CC = CerebralCortex()
users = CC.get_all_users("mperf-alabsi")
respiration_raw_autosenseble = "RESPIRATION--org.md2k.autosenseble--AUTOSENSE_BLE--CHEST"
for user in users:
    print(user['identifier'])
# activity_config.py

# -------------------- Fields --------------------
TASK_FIELDS = [
    "Id","WhoId","WhatId","Subject","ActivityDate","Status","Priority","Description","Type",
    "IsReminderSet","IsRecurrence","TaskSubtype","OPX_Start_Date__c",
    "Task_Complete_Date__c","Task_Type__c","Request__c","CTO_Flow__c",
    "OwnerId"
]


EVENT_FIELDS = [
    "Id", "Subject","StartDateTime", "EndDateTime","ActivityDate", "ReminderDateTime", "IsReminderSet", "WhatId", "WhoId",
    "Description", "IsRecurrence","CTO_Flow__c","ActivityDateTime","DurationInMinutes",
    "OwnerId"
]

# -------------------- Conditions --------------------
TASK_CONDITION = """
CreatedDate = TODAY AND Subject != 'BAT Is Mandatory For This Order/Request'
AND (
    What.Type IN ('Account','Impact_Tracker__c','Request__c','WorkOrder','Order','ServiceAppointment')
    OR (What.Type = null AND WhoId != null)
)
"""

EVENT_CONDITION = """
CreatedDate = TODAY
AND(
    What.Type IN ('Account','Impact_Tracker__c','ResourceAbsence')
    OR (What.Type = null AND WhoId != null)
)
"""
#CreatedDate > LAST_N_YEARS:2
# -------------------- Object Conditions --------------------
OBJECT_CONDITIONS = {
    "Account": "RecordType.Name IN ('Parent Company','Brand','Dealer') AND IsPersonAccount = false",
    "Impact_Tracker__c": "Clients_Brands__c!=null and Clients_Brands__r.RecordType.name in ('Parent Company','Brand','Dealer')",
    "Order": " RecordType.name in ('Field Win Win','Gift Card Procurement','Incentive')",
    "Request__c": "Brand__r.RecordType.name in ('Parent Company','Brand','Dealer') and Division__r.RecordType.name ='Brand Program')",
    "ServiceAppointment": "",
    "WorkOrder": "Field_Win_Win__c IN (select id from order where  RecordType.name in ('Field Win Win','Gift Card Procurement','Incentive'))"
}

# -------------------- Master Config --------------------
ACTIVITY_CONFIG = {
    "Task": {
        "fields": TASK_FIELDS,
        "condition": TASK_CONDITION.strip()
    },
    "Event": {
        "fields": EVENT_FIELDS,
        "condition": EVENT_CONDITION.strip()
    }
}

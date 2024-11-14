import mparticle
import os
import headers_constants as h
import value_formatter as format
from dotenv import load_dotenv

load_dotenv()


def build_batch_object(row, cols):
    batch = create_batch()
    batch.user_identities = create_identity(row, cols)
    batch.events = create_event(row, cols)
    batch.user_attributes = create_attributes(row, cols)
    return batch


def create_batch():
    """Creates a new mParticle batch"""

    batch = mparticle.Batch()
    batch.environment = os.getenv('MPARTICLE_ENVIRONMENT')
    if os.getenv('MPARTICLE_ENVIRONMENT'):
        batch.context = build_context()
    return batch


def build_context():
    batch_context = mparticle.BatchContext()
    MPARTICLE_DATA_PLAN_NAME = os.getenv('MPARTICLE_DATA_PLAN_NAME')
    MPARTICLE_DATA_PLAN_VERSION = os.getenv('MPARTICLE_DATA_PLAN_VERSION')
    if (MPARTICLE_DATA_PLAN_NAME and MPARTICLE_DATA_PLAN_VERSION):
        data_plan_ctx = mparticle.DataPlanContext(
            MPARTICLE_DATA_PLAN_NAME, int(MPARTICLE_DATA_PLAN_VERSION))
        batch_context.data_plan = data_plan_ctx
    return batch_context


def create_instance():
    """Configures a new mParticle EventsAPI instance

  This method creates a new mParticle EventsAPI instance
  containing the target host and configuration keys.

  Returns
  -------
  api_instance
      An instance of mParticle EventsAPI
  """

    config = mparticle.Configuration()
    config.api_key = os.getenv('MPARTICLE_API_KEY')
    config.api_secret = os.getenv('MPARTICLE_API_SECRET')
    config.debug = os.getenv('MPARTICLE_DEBUG') == 'true'
    config.host = os.getenv('MPARTICLE_HOST')
    config.connection_pool_size = 50

    api_instance = mparticle.EventsApi(config)
    return api_instance


def create_identity(data, cols):
    """Sets up mParticle Batch user identities

  Parameters
  ----------
  data: JSON
      a JSON object that contains user data

  Returns
  -------
  identities
      an instance of mParticle UserIdentities
      containing the user's email
  """

    identities = mparticle.UserIdentities()
    identities.customerid = process_value(data, cols, h.ID_CUSTOMERID)
    identities.email = process_value(data, cols, h.ID_EMAIL)
    identities.otherID2 = process_value(data, cols, h.ID_OTHER2)
    return identities


def create_event(data, cols):
    custom_attributes = {}

    policy_status = process_value(data, cols, h.EV_POLICYSTATUS)
    auto_renewal = process_value(data, cols, h.EV_AUTORENEWAL)
    individual_role = process_value(data, cols, h.EV_INDIVIDUALROLE)
    product = process_value(data, cols, h.EV_PRODUCT)
    brand = process_value(data, cols, h.EV_BRAND)
    is_multicar_policy = process_value(data, cols, h.EV_ISMULTICARPOL)
    online_policy = process_value(data, cols, h.EV_ONLINEPOL)
    vehicle_registration = process_value(data, cols, h.EV_VEHICLEREG)
    latest_renewal_date = process_value(data, cols, h.EV_LATESTRENEWDATE)
    latest_end_date = process_value(data, cols, h.EV_LATESTENDDATE)
    policy_number = process_value(data, cols, h.EV_POLICYNUMBER)
    payment_method = process_value(data, cols, h.EV_PAYMENTMETHOD)
    telephone = process_value(data, cols, h.EV_MOBILE)
    postcode = process_value(data, cols, h.EV_POSTCODE)
    types_of_ancillaries_held = process_value(data, cols, h.EV_ANCILLARYHOLDING)
    guidewire_account_ids = process_value(data, cols, h.EV_GUIDEWIREACCOUNTIDS)
    latest_start_date = process_value(data, cols, h.EV_LATESTSTARTDATE)

    if policy_status: custom_attributes["Policy Status"] = policy_status
    if auto_renewal: custom_attributes["Auto Renewal"] = auto_renewal
    if individual_role: custom_attributes["Individual Role"] = individual_role
    if product: custom_attributes["Product"] = product
    if brand: custom_attributes["Brand"] = brand
    custom_attributes["Is Multicar Policy"] = is_multicar_policy
    custom_attributes["Online Policy"] = online_policy
    if vehicle_registration: custom_attributes["Vehicle Registration"] = vehicle_registration
    if latest_renewal_date: custom_attributes["Latest Renewal Date"] = latest_renewal_date
    if latest_end_date: custom_attributes["Latest End Date"] = latest_end_date
    if policy_number: custom_attributes["Policy Number"] = policy_number
    if payment_method: custom_attributes["Payment Method"] = payment_method
    if telephone: custom_attributes["Telephone"] = telephone
    if postcode: custom_attributes["Postcode"] = postcode
    if types_of_ancillaries_held: custom_attributes["Types of Ancillaries Held"] = types_of_ancillaries_held
    if guidewire_account_ids: custom_attributes["Guidewire Account IDs"] = guidewire_account_ids
    if latest_start_date: custom_attributes["Latest Start Date"] = latest_start_date

    app_event = mparticle.AppEvent('Policy Imported', 'transaction', custom_attributes=custom_attributes)
    return [app_event]


def create_attributes(data, cols):
    user_attributes = {
        "$firstname": process_value(data, cols, h.AT_FIRSTNAME),
        "$lastname": process_value(data, cols, h.AT_LASTNAME),
        # "$mobile": process_value(data, cols, h.AT_MOBILE),
        "$mobile": process_value(data, cols, h.EV_MOBILE),
        "$zip": process_value(data, cols, h.EV_POSTCODE),
        "Guidewire Account IDs": process_value(data, cols, h.EV_GUIDEWIREACCOUNTIDS),
        "Types of Ancillaries Held": process_value(data, cols, h.EV_ANCILLARYHOLDING),
        "dob": process_value(data, cols, h.AT_BIRTHDATE)
    }

    print(user_attributes)
    return user_attributes


# def process_value(data, cols, header):
#     value = format.clean(data[cols.get_loc(header)])
#     if not value: return value
#
#
#     match header:
#         case h.ID_EMAIL:
#             return format.email(value)
#         case h.EV_ISMULTICARPOL:
#             return format.boolean(value)
#         case h.EV_ONLINEPOL:
#             return format.boolean(value)
#         case h.EV_LATESTRENEWDATE:
#             return format.datepol(value)
#         case h.EV_LATESTENDDATE:
#             return format.datepol(value)
#         case h.AT_FIRSTNAME:
#             return format.name(value)
#         case h.EV_MOBILE:
#             return format.phone(value)
#         case _:
#             return value
def process_value(data, cols, header):
    value = format.clean(data[cols.get_loc(header)])
    if not value: return value

    if header == h.ID_EMAIL:
        return format.email(value)
    elif header == h.EV_ISMULTICARPOL:
        return format.boolean(value)
    elif header == h.EV_ONLINEPOL:
        return format.boolean(value)
    elif header == h.EV_LATESTRENEWDATE:
        return format.datepol(value)
    elif header == h.EV_LATESTENDDATE:
        return format.datepol(value)
    elif header == h.AT_FIRSTNAME:
        return format.name(value)
    elif header == h.EV_MOBILE:
        return format.phone(value)
    else:
        return value
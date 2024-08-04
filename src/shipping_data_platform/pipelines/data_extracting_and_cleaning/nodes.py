"""
This is a boilerplate pipeline 'data_extracting_and_cleaning'
generated using Kedro 0.19.7
"""


import pandas as pd

def clean_containers_data(containers_df: pd.DataFrame) -> pd.DataFrame:
    
    """Ensure all timestamps are in correct format"""
    for column in ['endTime', 'dateOfLoading', 'created_at', 'updated_at', 'dateOfSynchronizationToErp', 'eta', 'currentEta', 'originalEta', 'dateOfDelivery', 'actualSailingDate', 'estimatedSailingDate', 'etaPodFinal', 'actualArrivalPod', 'finalEta', 'reachedDestinationAt', 'podDepartureTime', 'emptyContainerTime', 'manualEmptyContainerTime', 'manualCurrentEta', 'manualActualSailingDate', 'manualActualArrivalPod', 'dateOfDeliveryCreation', 'logisticCostUpdatedAt']:
        containers_df[column] = pd.to_datetime(containers_df[column], errors='coerce')
    
    """Ensure integer columns"""
    int_columns = ['id', 'createdById', 'buyOperationId', 'allocationId', 'sellOperationId', 'bookingId', 'flagId', 'shipmentId', 'pcBookingId', 'warehouseQualityId', 'stockpileId', 'inbound_ref_id']
    containers_df[int_columns] = containers_df[int_columns].apply(pd.to_numeric, errors='coerce', downcast='integer')
    
    """Ensure float columns"""
    float_columns = ['grossWeight', 'tareWeight', 'netWeight', 'weightSlip', 'maximumGrossWeight', 'quantity', 'finalMarginQuantity', 'estimatedMarginQuantity', 'totalFinalMarginQuantity', 'totalEstimatedMarginQuantity', 'totalDeliveredTons', 'deliveryTareWeightQuantity', 'deliveryGrossWeightQuantity', 'loadingInspectionCostQuantity', 'estimatedFreightCostQuantity', 'estimatedPreCarriageCostQuantity']
    containers_df[float_columns] = containers_df[float_columns].apply(pd.to_numeric, errors='coerce')
    
    """Ensure status column values"""
    valid_statuses = ['ALLOCATED', 'CLOSED', 'CONFIRMED', 'CANCELLED']
    containers_df = containers_df[containers_df['status'].isin(valid_statuses)]
    
    """Ensure non-null columns"""
    containers_df = containers_df.dropna(subset=['id', 'createdById', 'status', 'created_at', 'updated_at'])
    
    type_mapping = {
    'referenceNumber': 'string',
    'sealedNumber': 'string',
    'photos': 'string',
    'erpId': 'string',
    'erpRecId': 'string',
    'erpSubRecId': 'string',
    'quantityUnit': 'string',
    'preCarriageLine': 'string',
    'shippingLine': 'string',
    'estimatedLogisticCostVolume': 'string',
    'estimatedLogisticCostCurrency': 'string',
    'locationStatus': 'string',
    'etaStatus': 'string',
    'trackingEvent': 'string',
    'loadingOtherNumber': 'string',
    'deliverySlipNumber': 'string',
    'loadingComment': 'string',
    'deliveryComment': 'string',
    'mode': 'string',
    'deliveryDateComment': 'string',
    'loadingDateComment': 'string',
    'blNumber': 'string',
    'finalMarginCurrency': 'string',
    'finalMarginVolume': 'string',
    'estimatedMarginCurrency': 'string',
    'estimatedMarginVolume': 'string',
    'generalComment': 'string'
    }

    # Convert only the object type columns to their respective types based on PostgreSQL schema
    for col in containers_df.select_dtypes(include='object').columns:
        if col in type_mapping:
            dtype = type_mapping[col]
            if dtype == 'string':
                containers_df[col] = containers_df[col].astype('string')

    return containers_df


def clean_operations_data(operations_df: pd.DataFrame) -> pd.DataFrame:
    
    """Handle Missing Values"""
    """Fill missing values for `quantity` with 0 and for `dateOfConfirmation` with a placeholder date"""
    operations_df['quantity'].fillna(0, inplace=True)
    operations_df['dateOfConfirmation'].fillna(pd.Timestamp('1900-01-01'), inplace=True)

    """Drop rows where critical fields are missing"""
    operations_df.dropna(subset=['createdById', 'status', 'type'], inplace=True)

    """Consistency Checks"""
    # Validate categorical columns against predefined lists of valid values
    valid_statuses = ['IN_PROGRESS', 'CONFIRMED', 'CLOSED', 'NEW', 'CANCELLED']
    operations_df = operations_df[operations_df['status'].isin(valid_statuses)]

    valid_types = ['BUY', 'SELL']
    operations_df = operations_df[operations_df['type'].isin(valid_types)]

    valid_market_types = ['LOCAL', 'EXPORT']
    operations_df = operations_df[operations_df['marketType'].isin(valid_market_types)]

    """Transformations: Convert date columns to datetime format"""
    operations_df['dateOfConfirmation'] = pd.to_datetime(operations_df['dateOfConfirmation'], errors='coerce')
    operations_df['dateOfCreation'] = pd.to_datetime(operations_df['dateOfCreation'], errors='coerce')

    """Ensure numeric columns are of type float"""
    operations_df['quantity'] = pd.to_numeric(operations_df['quantity'], errors='coerce')
    operations_df['priceQuantity'] = pd.to_numeric(operations_df['priceQuantity'], errors='coerce')

    """Review Indexes and Constraints: Example: Checking for duplicate rows based on a unique identifier"""
    if operations_df.duplicated(subset=['id']).any():
        print("Duplicates found. Removing duplicates.")
        operations_df.drop_duplicates(subset=['id'], inplace=True)

    """Example: Ensuring no null values in critical foreign key columns"""
    if operations_df['createdById'].isnull().any():
        print("Missing values in 'createdById'. Consider filling or removing.")

    return operations_df
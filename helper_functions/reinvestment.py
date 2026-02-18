def call_option_contract_calculator(equity_value:float, option_price:float, underlier_price:float, coverage_ratio:float, partial_contract:bool=True):
    if partial_contract:
        contract_count = (equity_value / (100 * (underlier_price - option_price))) * coverage_ratio
    else:
        contract_count = 0
        cur_contract_count = round(((equity_value * coverage_ratio) / underlier_price) / 100)
        while cur_contract_count >= 1:
            contract_count += cur_contract_count
            cur_premium = cur_contract_count * option_price * 100
            cur_contract_count = round(((cur_premium * coverage_ratio) / underlier_price) / 100)
    return contract_count

def call_option_contract_calculator_new(equity_value:float, option_price:float, underlier_price:float, coverage_ratio:float, partial_contract:bool=True):
    if partial_contract:
        contract_count = (equity_value / (100 * (underlier_price - option_price))) * coverage_ratio
    else:
        contract_count = 0
        cur_contract_count = round(((equity_value * coverage_ratio) / underlier_price) / 100)
        while cur_contract_count <= -1:
            contract_count += cur_contract_count
            cur_premium = -cur_contract_count * option_price * 100
            cur_contract_count = round(((cur_premium * coverage_ratio) / underlier_price) / 100)
    return contract_count
            
            
            
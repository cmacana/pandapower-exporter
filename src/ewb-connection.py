import pandas as pd
import requests
import pandapower as pp

ewb_url = 'http://dev.cppal.ednar.net:9002'
output_path = ''


def get_asset(asset_id):
    asset = {}
    request = ewb_url + "/ewb/network/api/v1/assets/" + asset_id.__str__()
    data = requests.get(request).json()
    print("API request: ", request)
    if "assets" in data:
        asset = data["assets"]
    if "errors" in data:
        # displaying the warning message
        message = str(data["errors"])
        asset = None
    return asset


def get_asset_conn_nodes(asset_id):
    asset = get_asset(asset_id=asset_id)
    conns = asset[0]['connections']
    conns_df = pd.DataFrame(conns)
    return conns_df


class Feeder:
    def __init__(self, feeder_id):
        self.feeder_id = feeder_id
        self.data = self.get_data()
        self.infeeder_id = self.get_infeeder_id()
        self.assets = self.get_assets()
        if self.assets is not None:
            self.buses = self.get_buses()
        self.assets_df = pd.DataFrame(self.assets)
        self.head_cn = self.get_head_cn()
        self.base_voltage = self.get_base_voltage()
        self.net = pp.create_empty_network()
        self.min_line_length_km = 0.01

    def get_data(self):
        request = ewb_url + "/ewb/network/api/v1/feeder-assets/feeder/" + self.feeder_id.__str__()
        print("API request: ", request)
        data = requests.get(request).json()
        return data

    def get_assets(self):
        assets = {}
        if "feeders" in self.data:
            assets = self.data["feeders"][0]["assets"]
            pd.DataFrame(assets).to_csv(output_path + 'assets.csv')
        if "errors" in self.data:
            # displaying the warning message
            message = str(self.data["errors"])
            assets = None
        return assets

    def get_infeeder_id(self):
        if "feeders" in self.data:
            infeeder_id = self.data["feeders"][0]["infeeds"][0]
            print("Infeeder ID: ", infeeder_id)
        else:
            infeeder_id = None
        return infeeder_id

    def get_buses(self):
        df = pd.DataFrame()
        for asset in self.assets:
            conn = asset["connections"]
            voltage = asset["voltage"]
            for i, cn in enumerate(conn):
                if cn['lngLat'] is None:
                    lat = None
                    lon = None
                else:
                    lat = float(cn['lngLat']['longitude'])
                    lon = cn['lngLat']['latitude']
                df = df.append({'name': cn['connectivityNodeId'], 'vn_kv': voltage, 'busgeodata': (lat, lon)},
                               ignore_index=True)
        return df.drop_duplicates(subset="name").reset_index()

    def get_head_cn(self):
        is_head_asset = self.assets_df['id'] == str(self.infeeder_id)
        head_asset = self.assets_df[is_head_asset]
        return head_asset['connections'].values[0][0]['connectivityNodeId']

    def get_base_voltage(self):
        base_voltage = self.buses[self.buses['name'] == self.head_cn]['vn_kv'].values[0]
        return base_voltage

    def create_pp_buses(self):
        for i, bus in self.buses.iterrows():
            pp.create_bus(self.net, name=bus["name"], vn_kv=bus["vn_kv"] / 1000, busgeodata=bus["busgeodata"])
        pd.DataFrame(self.net.bus).to_csv(output_path + "pp_buses.csv")
        return self.net.bus

    def create_pp_lines(self):
        conductors = self.assets_df[self.assets_df['type'] == "Conductor"]
        for _, conductor in conductors.iterrows():
            from_cn_id = conductor["connections"][0]['connectivityNodeId']
            to_cn_id = conductor["connections"][1]['connectivityNodeId']
            from_bus = pp.get_element_index(self.net, "bus", from_cn_id)
            to_bus = pp.get_element_index(self.net, "bus", to_cn_id)
            length = conductor["length"]
            if length == 0:
                length = self.min_line_length_km
            line_name = from_cn_id + "-" + to_cn_id
            pp.create_line(net=self.net, name=line_name, from_bus=from_bus, to_bus=to_bus, length_km=length,
                           std_type="NAYY 4x50 SE")
        pd.DataFrame(self.net.line).to_csv(output_path + "pp_lines.csv")
        conductors.to_csv(output_path + "ewb_conductors.csv")
        return self.net.line

    def create_pp_transformers(self):
        distTrafos = self.assets_df[self.assets_df['type'] == "DistTransformer"]
        powerTrafos = distTrafos.drop_duplicates(subset="name").reset_index()
        for _, trafo in powerTrafos.iterrows():
            hv_bus = pp.get_element_index(self.net, "bus", trafo["connections"][0]['connectivityNodeId'])
            lv_bus = pp.get_element_index(self.net, "bus", trafo["connections"][1]['connectivityNodeId'])
            pp.create_transformer(self.net, hv_bus=hv_bus, lv_bus=lv_bus, std_type="0.4 MVA 20/0.4 kV")
        pd.DataFrame(self.net.trafo).to_csv(output_path + "pp_trafos.csv")
        powerTrafos.to_csv(output_path + "ewb_trafos.csv")
        return self.net.trafo

    def get_connections(self):
        conn = self.assets_df["connections"]
        conn.to_csv(output_path + "connections.csv")
        return conn

    def create_pp_extgrid(self):
        head_bus = self.net.bus[self.net.bus['name'] == self.head_cn]
        pp.create_ext_grid(net=self.net, bus=head_bus.index.values[0], vm_pu=1, va_degree=0)
        return self.net.ext_grid


testFeeder = Feeder(feeder_id="AL002")
# pp_buses = testFeeder.create_pp_buses()
# pp_extgrid = testFeeder.create_pp_extgrid()
# pp_trafos = testFeeder.create_pp_transformers()
# pp_line = testFeeder.create_pp_lines()
# pp.create_load(testFeeder.net, bus=0, p_mw=1, q_mvar=0.5,const_z_percent=0,const_i_percent=0,name="Load1")
# pp.create_load(testFeeder.net, bus=14, p_mw=1, q_mvar=0.5, name="Load2")
# pp_load = testFeeder.net.load
# pp.runpp(testFeeder.net)
# print(testFeeder.net)
# lf_res = testFeeder.net.res_bus
# lf_res.to_csv("lf_res_bus")

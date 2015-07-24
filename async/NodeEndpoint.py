class NodeEndpoint:
    protocol = ''
    host = ''
    port = ''

    @staticmethod
    def from_parameters(protocol, host, port):
        endpoint = NodeEndpoint()
        endpoint.protocol = protocol
        endpoint.host = host
        endpoint.port = port
        return endpoint

    @staticmethod
    def from_json(endpoint_json):
        return NodeEndpoint.from_parameters(
            endpoint_json['protocol'],
            endpoint_json['host'],
            endpoint_json['port'])

    def url(self):
        return '{0}://{1}:{2}/'.format(self.protocol, self.host, self.port)
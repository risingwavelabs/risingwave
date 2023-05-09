from pyarrow.flight import FlightClient
import sys


def check_udf_service_available(addr: str) -> bool:
    """Check if the UDF service is available at the given address."""
    try:
        client = FlightClient(f"grpc://{addr}")
        client.wait_for_available()
        return True
    except Exception as e:
        print(f"Error connecting to RisingWave UDF service: {str(e)}")
        return False


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python3 health_check.py <server_address>")
        sys.exit(1)

    server_address = sys.argv[1]
    if check_udf_service_available(server_address):
        print("OK")
    else:
        print("unavailable")
        exit(-1)

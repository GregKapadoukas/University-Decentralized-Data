import hashlib
import re

from node.request import send_command, send_lookup_command, send_store_command


def split_command(command):
    pattern = r'"[^"]*"|\'[^\']*\'|\S+'
    matches = re.findall(pattern, command)
    return [m[1:-1] if m.startswith('"') or m.startswith("'") else m for m in matches]


def console(processes, exit_flag):
    print("Starting Console:")
    print("Enter command in the following format:")
    print(
        'For lookups: "node_host" "node_port" "lookup" "Institution Name" "Number of Awards"'
    )
    print(
        'For storing: "node_host" "node_port" "store" "Institution Name" "Number of Awards" "Name of Computer Scientist"'
    )
    print('For others: "node_host" "node_port" "command"')
    print('To exit type "exit"')
    print('Example command: "localhost 8080 lookup MIT 15"')
    while True:
        msg = input("# ")
        if msg == "exit":
            if exit_flag:
                for process in processes:
                    process.terminate()
                    process.join()
            break
        msg = split_command(msg)
        if len(msg) >= 3:
            try:
                if msg[2] == "lookup" and len(msg) == 5:
                    results = send_lookup_command(
                        str(msg[0]),
                        int(msg[1]),
                        int(hashlib.md5((msg[3]).encode()).hexdigest(), 16),
                        str(msg[4]),
                    )
                    print(
                        f"Result for Institution: {msg[-2]} and #Awards: {msg[-1]} is:"
                    )
                    for result in results:
                        print(result)

                elif msg[2] == "store" and len(msg) >= 6:
                    send_store_command(
                        str(msg[0]),
                        int(msg[1]),
                        int(hashlib.md5((msg[3]).encode()).hexdigest(), 16),
                        str(msg[4]),
                        str(msg[5]),
                    )
                else:
                    send_command(str(msg[2]), str(msg[0]), int(msg[1]))
            except Exception:
                print("Connection Error: Please Try Again")
                continue
        else:
            print("Invalid Command")

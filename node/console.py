from node.request import send_command


def console(processes, exit_flag):
    while True:
        msg = input("# ")
        if msg == "exit":
            if exit_flag:
                for process in processes:
                    process.terminate()
                    process.join()
            break
        msg = msg.split(" ", 2)
        if len(msg) == 3:
            send_command(str(msg[2]), str(msg[0]), int(msg[1]))
        else:
            print("Invalid Command")

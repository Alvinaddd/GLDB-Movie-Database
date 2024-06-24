package cli;

import tributary.Tributary;
import tributary.Topic;
//import tributary.Consumer;
import tributary.Partition;
//import tributary.Consumer;
//import tributary.Producer;

public class CLI {

    private final Tributary tributary;

    public CLI() {
        this.tributary = new Tributary();
    }

    public void parseCommand(String command) {
        String[] commandParts = command.split(" ");
        String action = commandParts[0];

        switch (action) {
            case "create":
            handleCreateCommand(commandParts);
            break;
            case "delete":
           // handleDeleteCommand(commandParts);
            break;
            case "produce":
            //handleProduceCommand(commandParts);
            break;
            case "consume":
            //handleConsumeCommand(commandParts);
            break;
            case "show":
           // handleShowCommand(commandParts);
            break;
            // Handle other commands...
            default:
                System.out.println("Unknown command: " + command);
        }
    }

    private void handleCreateCommand(String[] args) {
        String entityType = args[1];
        switch (entityType) {
            case "topic":
                Topic topic = tributary.createTopic(args[2], args[3]);
                System.out.println("Created topic with Id: " + topic.getId() + " with type: " + topic.getType());
                break;
         
            case "partition":
                Partition partition = tributary.createPartition(args[2], args[3]);
                System.out.println("Created partition: " + partition);
                break;
            /* 
            case "consumer":
                Consumer consumer = tributary.createConsumer(args[2], args[3]);
                System.out.println("Created consumer: " + consumer);
                break;
            case "producer":
                Producer producer = tributary.createProducer(args[2], args[3], args[4]);
                System.out.println("Created producer: " + producer);
                break;
            */
            
            default:
                System.out.println("Unknown entity type: " + entityType);
        }
    }

}

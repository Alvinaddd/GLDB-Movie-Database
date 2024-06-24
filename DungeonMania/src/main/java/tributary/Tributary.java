package tributary;

import java.util.HashMap;
import java.util.Map;

public class Tributary {
    private static Tributary instance = null;

    //stores topics 
    private Map<String, Topic> topics;
    private Map<String, Group> groups;
    private Map<String, Producer> producers;

    public Tributary() {
        this.topics = new HashMap<>();
        this.groups = new HashMap<>(); 
        this.producers = new HashMap<>(); 
    }
    // only one tributary
    public static Tributary getInstance() {
        if (instance == null) {
            instance = new Tributary();
        }
        return instance;
    }

    public static void resetInstance() {
        instance = null;
    }

    public Topic createTopic(String id, String type) {
        System.out.println("Creating topic with ID: " + id + ", type: " + type);

        if (topics.containsKey(id)) {
            System.out.println("Topic with id: " + id + " already exists.");
            return null;
        }
        Topic topic = new Topic(id, type);
        this.topics.put(id, topic);
        System.out.println("Created topic with id: " + id + " and type: " + type);
        return topic;
    }

    public Topic getTopicById(String id) {
        return topics.get(id);
    }

    public Partition createPartition(String topicId, String partitionId) {
        //check if topics exist 
        Topic topic = getTopicById(topicId);
        if (topic == null) {
            System.out.println("Topic with id: " + topicId + " does not exist.");
            return null;
        }
        // if topic exist create partition 
        Partition partition = topic.createPartition(topicId, partitionId);
        return partition; 
    }

    public Group createGroup(String groupId, String topicId, String rebalancing){
        //check that groupId is unique 
        if (groups.containsKey(groupId)) {
            System.out.println("Group with id: " + groupId + " already exists.");
            return null;
        }

        //check topicId exist 
        if (!topics.containsKey(topicId)) {
            System.out.println("Topic with id: " + topicId + " does not exist.");
            return null;
        }

        if (!rebalancing.equals("Range") && !rebalancing.equals("RoundRobin")) {
            System.out.println("Invalid rebalancing method");
            return null;
        }

        //create the Group
        Group group = new Group(groupId, topicId,rebalancing);
        this.groups.put(groupId, group);
        System.out.println("Created consumer group with id: " + groupId + " and rebalancing: " + rebalancing + " for the topic " + topicId);
        return group;
    }

    public Consumer createConsumer(String groupId, String consumerId){
        //check if groupId exsit 
        if (!groups.containsKey(groupId)) {
            System.out.println("Group with id: " + groupId + "does not exists.");
            return null;
        }

        //create the consumer
        Group group = getGroupById(groupId);
        Consumer consumer = group.createConsumer(consumerId); 
        return consumer;
    }

    public Group getGroupById(String groupId) {
        return groups.get(groupId);
    }

    public Boolean deleteConsumer(String consumerId) {
        //search the group that the consumer belongs to 
        Group group = findConsumerGroupById(consumerId);
        if (group == null) {
            System.out.println("Group not found for consumer ID: " + consumerId);
            return false;
        }
        group.deleteConsumer(consumerId);
        return true;
    }

    private Group findConsumerGroupById(String consumerId) {
        for (Group group :groups.values()) {
            if (group.hasConsumer(consumerId)) {
                return group;
            }
        }
        return null;
    }

    public Producer createProducer(String producerId, String type, String allocation) {
         //check that producer is unique 
         if (producers.containsKey(producerId)) {
            System.out.println("producer with id: " + producerId + " already exists.");
            return null;
        }
        if (!allocation.equals("Random") && !allocation.equals("Manual")) {
            System.out.println("Invalid allocation method");
            return null;
        }

        //create Producer 
        Producer producer = new Producer(producerId, type, allocation);
        this.producers.put(producerId, producer);
        System.out.println("Created producer with id: " + producerId);
        return producer;
    }
    

}

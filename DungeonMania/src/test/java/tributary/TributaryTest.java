package tributary;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

public class TributaryTest {
    private Tributary tributary;

    @BeforeEach
    public void setUp() {
        tributary = Tributary.getInstance();
    }
    @AfterEach
    public void reset() {
        Tributary.resetInstance();
    }

    @Test
    public void testCreateTopicWithStringType() {
        // set up 
        String topicId = "Topic_A";
        String topicType = "String";
    
        // create topic
        Topic topic = tributary.createTopic(topicId, topicType);

        //check 
        assertNotNull(tributary.getTopicById(topicId), "Topic is not created");
        assertEquals(topicId, topic.getId(), "Topic ID should match");
        assertEquals(topicType, topic.getType(), "Topic type should match");

        //check for duplicates 
        Topic topic2 = tributary.createTopic(topicId, topicType);
        assertNull(topic2, "Topic is not created due to duplicate");

    }

    @Test
    public void testCreatePartition() {
        // set up 
        String partitionId = "Partition_0";
        String topicId = "Topic_A";
        String topicType = "String";
        tributary.createTopic(topicId, topicType);
        Topic topic = tributary.getTopicById(topicId);

        //create partition 
        Partition partition = tributary.createPartition(topicId, partitionId);

        //check 
        assertNotNull(partition, "parition is not created");
        assertEquals(topic.getId(), partition.getTopicId(), "Topic ID should match");
        assertEquals(partitionId, partition.getPartitionId(), "partition ID should match");

        //check for duplicates
        Partition partition2 = tributary.createPartition(topicId, partitionId);
        assertNull(partition2, "partition is not created due to duplicate");
    }

    @Test
    public void testCreateGroup() {
        // set up 
        String groupId = "Group_A";
        String groupId3 = "Group_B";
        String rebalancing = "Range";
        String invalidRebalancing = "invalid";  

        String topicId = "Topic_A";
        String topicType = "String";
        tributary.createTopic(topicId, topicType);
       
        //cretae Group
        Group group = tributary.createGroup(groupId, topicId, rebalancing);

        //check 
        assertNotNull(group, "Group is not created");
        assertEquals(topicId, group.getTopicId(), "Topic ID should match");
        assertEquals(groupId, group.getGroupId(), "Group ID should match");
        assertEquals(rebalancing, group.getRebalancing(), "rebalancing should match");

        //check for duplicates
        Group group2 = tributary.createGroup(groupId, topicId, rebalancing);
        assertNull(group2, "Group is not created due to duplicate");

        //check for invalid rebalancing method
        Group group3 = tributary.createGroup(groupId3, topicId, invalidRebalancing);
        assertNull(group3, "Group is not created due to invalid rebalancing");
    }

    @Test
    public void testCreateConsumer() {
        // set up 
        String groupId = "Group_A";
        String rebalancing = "Range";
        String topicId = "Topic_A";
        String topicType = "String";
        String consumerId = "Consumer_I"; 

        tributary.createTopic(topicId, topicType);
        tributary.createGroup(groupId, topicId, rebalancing);

        //create consumer 
        Consumer consumer = tributary.createConsumer(groupId, consumerId);

        //check 
        assertNotNull(consumer, "consumer is not created");
        assertEquals(groupId, consumer.getGroupId(), "Group ID should match");
       
    }

    @Test
    public void testdeleteConsumer() {
        // set up 
        String groupId = "Group_A";
        String rebalancing = "Range";
        String topicId = "Topic_A";
        String topicType = "String";
        String consumerId = "Consumer_I"; 

        tributary.createTopic(topicId, topicType);
        Group group = tributary.createGroup(groupId, topicId, rebalancing);
        tributary.createConsumer(groupId, consumerId);

        //delete consumer 
        Boolean consumerDelete = tributary.deleteConsumer(consumerId);

        //check 
        assertTrue(consumerDelete, "consumer deleted");
       //check that it no longer contains 
       assertFalse(group.hasConsumer(consumerId), "consumer id has already been deleted");  
    }


    @Test
    public void testCreateproducer() {
        // set up 
        String producerId = "Producer_A";
        String type = "String"; 
        String validAllocation = "Random";
        String invalidAllocation = "Invalid";

        // create a producer 
        Producer producer = tributary.createProducer(producerId, type, validAllocation);

        // check
        assertNotNull(producer, "Producer is not created");
        assertEquals(producerId, producer.getProducerId(), "Producer ID should match");
        assertEquals(type, producer.getType(), "Type should match");
        assertEquals(validAllocation, producer.getAllocation(), "Allocation method should match");

        // check for duplicates
        Producer duplicateProducer = tributary.createProducer(producerId, type, validAllocation);
        assertNull(duplicateProducer,  "Producer should not be created due to duplicate ID");

        // check for invalid allocation method
        Producer invalidAllocationProducer = tributary.createProducer(producerId, type, invalidAllocation);
        assertNull(invalidAllocationProducer, "Producer should not be created due to invalid allocation method");
        
    }
    
}

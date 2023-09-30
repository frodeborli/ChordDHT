using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.ObjectModel;
using ChordDHT.ChordProtocol;

[TestClass]
public class UnitTest
{
    [TestMethod]
    public void TestAddNode()
    {
        IChord chordInstance = new Chord("TestNode");

        // Add new node
        chordInstance.addNode("TestNode1");
        CollectionAssert.Contains(chordInstance.knowNodes.ToList(), "TestNode1");
        Assert.AreEqual(1, chordInstance.knownNodes.Count);

        // Ignore duplicate
        chordInstance.addNode("TestNode1");
        Assert.AreEqual(1, chordInstance.knownNodes.Count);
    }
    /*
    [TestMethod]
    public void TestRemoveNode_NodeExists() 
    {
        IChord chordInstance = new Chord("TestNode");
        chordInstance.addNode("NodeToBeRemoved");

        chordInstance.removeNode("NodeToBeRemoved");

        CollectionAssert.DoesNotContain(chordInstance.knownNodes.ToList(), "NodeToBeRemoved");
    }

    [TestMethod]
    public void TestRemoveNode_NodeDoesNotExist()
    {
        IChord chordInstance = new Chord("TestNode");
        int initialCount = chordInstance.knownNodes.Count;

        chordInstance.removeNode("NodeToBeRemoved");

        Assert.AreEqual(initialCount, chordInstance.knownNodes.Count);
    }
    */
}


public void removeNode(string nodeName)
{
    if (this.knownNodes.IndexOf(nodeName) == -1)
    {
        // Ignore nodes that don't exist
        return;
    }
    this.knownNodes.Remove(nodeName);
    this.updateFingersTable();
}
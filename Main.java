

package org.apache.calcite.test;

import java.util.*;

public class Main {
  static class Edge {
    private String name;
    private Node source;
    private Node destination;

    public Edge(String name, Node source, Node destination) {
      this.name = name;
      this.source = source;
      this.destination = destination;
    }

    public String getName() {
      return name;
    }

    public Node getSource() {
      return source;
    }

    public Node getDestination() {
      return destination;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Edge edge = (Edge) o;
      return Objects.equals(name, edge.name) &&
          Objects.equals(source, edge.source) &&
          Objects.equals(destination, edge.destination);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, source, destination);
    }

    @Override
    public String toString() {
      return "Edge(" + name + ": " + source + " -> " + destination + ")";
    }
  }

  static class Node {
    private String content;
    private Map<Node, Edge> neighbors; // Maps neighbor node to the edge connecting them

    public Node(String content) {
      this.content = content;
      this.neighbors = new HashMap<>();
    }

    public void addNeighbor(Node neighbor, Edge edge) {
      neighbors.put(neighbor, edge);
    }

    public String getContent() {
      return content;
    }

    public Collection<Node> getNeighbors() {
      return neighbors.keySet();
    }

    public Edge getEdgeTo(Node neighbor) {
      return neighbors.get(neighbor);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Node node = (Node) o;
      return Objects.equals(content, node.content);
    }

    @Override
    public int hashCode() {
      return Objects.hash(content);
    }

    @Override
    public String toString() {
      return "Node(" + content + ")";
    }
  }

  static class Graph {
    private List<Node> nodes;
    private Map<String, Node> nodeMap;
    private List<Edge> edges;

    public Graph() {
      this.nodes = new ArrayList<>();
      this.nodeMap = new HashMap<>();
      this.edges = new ArrayList<>();
    }

    public Node addNode(String content) {
      if (nodeMap.containsKey(content)) {
        throw new IllegalArgumentException("Node with content '" + content + "' already exists");
      }
      Node node = new Node(content);
      nodes.add(node);
      nodeMap.put(content, node);
      return node;
    }

    public void addEdge(String srcContent, String destContent, String name) {
      // Get or create source node
      Node srcNode = nodeMap.get(srcContent);
      if (srcNode == null) {
        srcNode = addNode(srcContent);
      }

      // Get or create destination node
      Node destNode = nodeMap.get(destContent);
      if (destNode == null) {
        destNode = addNode(destContent);
      }

      Edge edge = new Edge(name, srcNode, destNode);
      edges.add(edge);
      srcNode.addNeighbor(destNode, edge);
      destNode.addNeighbor(srcNode, edge); // Since it's undirected
    }

    public Set<Node> findSteinerTree(Set<String> terminals) {
      if (nodes.isEmpty()) {
        return Collections.emptySet();
      }

      int V = nodes.size();
      int n = terminals.size();
      String[] terminalArray = terminals.toArray(new String[0]);

      // dp[mask][v] represents the minimum number of edges needed to connect vertices in mask and vertex v
      int[][] dp = new int[1 << n][V];
      // parent[mask][v] stores the information needed to reconstruct the solution
      int[][][] parent = new int[1 << n][V][2]; // [mask][v][0] = submask, [mask][v][1] = vertex index

      for (int[] row : dp) {
        Arrays.fill(row, Integer.MAX_VALUE);
      }
      for (int[][] maskParents : parent) {
        for (int[] vertexParent : maskParents) {
          Arrays.fill(vertexParent, -1);
        }
      }

      // Initialize single terminal cases
      for (int i = 0; i < n; i++) {
        String terminal = terminalArray[i];
        int mask = 1 << i;

        // Find shortest paths from this terminal to all vertices
        Map<Node, Integer> dist = bfs(nodeMap.get(terminal));

        for (int v = 0; v < V; v++) {
          Node currentNode = nodes.get(v);
          Integer distance = dist.get(currentNode);
          if (distance != null) {
            dp[mask][v] = distance;
            parent[mask][v][0] = -1;  // Indicates direct path
            parent[mask][v][1] = nodes.indexOf(nodeMap.get(terminal));  // Store source terminal index
          }
        }
      }

      // Iterate through all possible subsets of terminals
      for (int mask = 1; mask < (1 << n); mask++) {
        if (Integer.bitCount(mask) <= 1) continue;

        for (int v = 0; v < V; v++) {
          // Try all possible ways to split the current set
          for (int submask = (mask - 1) & mask; submask > 0; submask = (submask - 1) & mask) {
            int complementMask = mask ^ submask;
            if (dp[submask][v] != Integer.MAX_VALUE && dp[complementMask][v] != Integer.MAX_VALUE) {
              int totalCost = dp[submask][v] + dp[complementMask][v];
              if (totalCost < dp[mask][v]) {
                dp[mask][v] = totalCost;
                parent[mask][v][0] = submask;
                parent[mask][v][1] = v;
              }
            }
          }
        }
      }

      // Find the optimal solution
      int finalMask = (1 << n) - 1;
      int minEdges = Integer.MAX_VALUE;
      int bestVertex = -1;

      for (int v = 0; v < V; v++) {
        if (dp[finalMask][v] < minEdges) {
          minEdges = dp[finalMask][v];
          bestVertex = v;
        }
      }

      // Reconstruct the solution
      Set<Node> steinerVertices = new HashSet<>();
      reconstructSolution(finalMask, bestVertex, parent, steinerVertices, terminals);

      return steinerVertices;
    }

    private void reconstructSolution(int mask, int vertexIndex, int[][][] parent, Set<Node> result, Set<String> terminals) {
      if (mask == 0) return;
      Node currentNode = nodes.get(vertexIndex);
      result.add(currentNode);

      if (parent[mask][vertexIndex][0] == -1) {
        // This is a direct path from a terminal
        int terminalIndex = parent[mask][vertexIndex][1];
        Node terminalNode = nodes.get(terminalIndex);

        // Add all vertices in the path from terminal to vertex using BFS
        Queue<Node> queue = new LinkedList<>();
        Map<Node, Node> prev = new HashMap<>();
        queue.offer(terminalNode);
        Set<Node> visited = new HashSet<>();
        visited.add(terminalNode);

        while (!queue.isEmpty() && !visited.contains(currentNode)) {
          Node u = queue.poll();
          for (Node v : u.getNeighbors()) {
            if (!visited.contains(v)) {
              visited.add(v);
              prev.put(v, u);
              queue.offer(v);
            }
          }
        }

        // Reconstruct path
        Node current = currentNode;
        while (!current.equals(terminalNode)) {
          result.add(current);
          current = prev.get(current);
        }
        result.add(terminalNode);
      } else {
        // This is a merge of two submasks
        int submask = parent[mask][vertexIndex][0];
        reconstructSolution(submask, parent[mask][vertexIndex][1], parent, result, terminals);
        reconstructSolution(mask ^ submask, parent[mask][vertexIndex][1], parent, result, terminals);
      }
    }

    private Map<Node, Integer> bfs(Node start) {
      Map<Node, Integer> dist = new HashMap<>();
      for (Node node : nodes) {
        dist.put(node, Integer.MAX_VALUE);
      }
      dist.put(start, 0);

      Queue<Node> queue = new LinkedList<>();
      queue.offer(start);

      while (!queue.isEmpty()) {
        Node u = queue.poll();
        for (Node v : u.getNeighbors()) {
          if (dist.get(v) == Integer.MAX_VALUE) {
            dist.put(v, dist.get(u) + 1);
            queue.offer(v);
          }
        }
      }
      return dist;
    }
  }

  public static void main(String[] args) {
    // Example usage
    Graph g = new Graph();

    // Adding edges will automatically create nodes
    g.addEdge("A", "B", "AB_Connection");
    g.addEdge("A", "F", "AF_Connection");
    g.addEdge("B", "C", "BC_Connection");
    g.addEdge("C", "D", "CD_Connection");
    g.addEdge("B", "E", "BE_Connection");
    g.addEdge("E", "D", "ED_Connection");
    g.addEdge("E", "F", "EF_Connection");

    // Define terminal vertices using node content
    Set<String> terminals = new HashSet<>(Arrays.asList("A", "B", "F"));

    // Find Steiner tree vertices
    Set<Node> result = g.findSteinerTree(terminals);
    System.out.println("Vertices in the Steiner tree: " +
        result.stream().map(Node::getContent).collect(java.util.stream.Collectors.toSet()));

    // Print the edges in the Steiner tree
    System.out.println("Edges in the Steiner tree:");
    for (Node node : result) {
      for (Node neighbor : node.getNeighbors()) {
        if (result.contains(neighbor) && node.getContent().compareTo(neighbor.getContent()) < 0) {
          Edge edge = node.getEdgeTo(neighbor);
          System.out.println(edge.getName());
        }
      }
    }
  }
}

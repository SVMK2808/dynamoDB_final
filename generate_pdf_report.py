from fpdf import FPDF
import os

class PDF(FPDF):
    def header(self):
        pass

    def footer(self):
        self.set_y(-15)
        self.set_font('Times', 'I', 8)
        self.cell(0, 10, 'Page ' + str(self.page_no()) + '/{nb}', 0, 0, 'C')

    def chapter_title(self, title):
        self.set_font('Times', 'B', 16)
        self.cell(0, 6, title, 0, 1, 'L')
        self.ln(4)

    def chapter_body(self, body):
        self.set_font('Times', '', 12)
        self.multi_cell(0, 5, body)
        self.ln()

    def add_image(self, image_path, caption):
        if os.path.exists(image_path):
            # Check available vertical space
            # A4 height is 297mm. Footer is at -15mm.
            # We estimate the image block needs about 90-100mm (Image ~80mm + Caption ~10mm)
            # If current Y is > 190, we might run out of space, so add a new page.
            if self.get_y() > 190:
                self.add_page()

            # Center the image
            # A4 Width = 210mm
            # Image Width = 130mm (Reduced from 190mm)
            # X = (210 - 130) / 2 = 40
            self.image(image_path, x=40, w=130)
            self.ln(2)
            
            # Caption
            self.set_font('Times', 'I', 10)
            self.cell(0, 10, caption, 0, 1, 'C')
            self.ln(5)
        else:
            print(f"Warning: Image not found {image_path}")

pdf = PDF()
pdf.alias_nb_pages()
pdf.add_page()

# Title
pdf.set_font('Times', 'B', 24)
pdf.cell(0, 10, 'DynamoDB Implementation Analysis Report', 0, 1, 'C')
pdf.ln(5)

# Subtitle
pdf.set_font('Times', '', 12)
pdf.cell(0, 10, 'S V Mohit Kumar (2024201010), Morampudi Srisai Krishna (2022101115), Abradeep Das(2024202018)', 0, 1, 'C')
pdf.ln(10)

# 1. Introduction
pdf.chapter_title('1. Introduction')
pdf.chapter_body(
    "This report provides a comprehensive analysis of a distributed key-value store implementation inspired by Amazon's Dynamo. "
    "The system is built in Go and demonstrates core distributed systems principles such as consistent hashing, vector clocks, "
    "gossip protocols, and Merkle tree-based anti-entropy.\n\n"
    "The codebase consists of a Go backend that handles the core logic and Python scripts for theoretical simulation and benchmarking. "
    "This report covers the architectural components, implementation details, and a theoretical performance analysis based on simulation results."
)

# 2. Architecture & Implementation
pdf.chapter_title('2. Architecture & Implementation')
pdf.chapter_body(
    "The system architecture follows a decentralized, peer-to-peer model where all nodes are equal. Key components include:\n\n"
    "2.1 Consistent Hashing\n"
    "Data partitioning is handled by a consistent hash ring. The implementation uses virtual nodes (256 per physical node) to ensure "
    "even distribution of keys and load balancing.\n\n"
    "2.2 Replication (N, R, W)\n"
    "The system supports configurable replication factor (N) and quorum consistency levels (R for reads, W for writes).\n\n"
    "2.3 Vector Clocks\n"
    "To handle concurrent updates, the system uses Vector Clocks to track causal ordering and detect conflicts.\n\n"
    "2.4 Gossip Protocol\n"
    "Membership management and failure detection are handled via a Gossip protocol.\n\n"
    "2.5 Anti-Entropy (Merkle Trees)\n"
    "To synchronize replicas and repair inconsistencies, the system uses Merkle Trees."
)

# 3. Performance Analysis
pdf.chapter_title('3. Performance Analysis')
pdf.chapter_body(
    "A theoretical simulation was conducted to evaluate the system's performance under various conditions. "
    "The analysis is divided into three parts: Impact of Cluster Size, Impact of Replication Factor (N), and Detailed Consistency Analysis."
)

# 3.1 Impact of Cluster Size
pdf.chapter_title('3.1 Impact of Cluster Size')
pdf.chapter_body(
    "This section analyzes how the system scales with different cluster sizes.\n\n"
    "Experimental Setup:\n"
    "- Constants: Replication Factor (N=3), Read Quorum (R=2), Write Quorum (W=2).\n"
    "- Variables: Cluster Size (100, 500, 1000 nodes), Node Failure Rate (0% - 20%)."
)

pdf.add_image('backend/part1_write_throughput.png', 'Figure 1: Write Throughput vs. Failure Rate (Different Cluster Sizes)')
pdf.chapter_body(
    "Figure 1 Analysis: Write throughput remains remarkably consistent across cluster sizes of 100, 500, and 1000 nodes. "
    "This demonstrates the effectiveness of the O(1) consistent hashing routing. "
    "However, throughput declines linearly as the node failure rate increases, as the coordinator must wait for timeouts or retry with other nodes."
)

pdf.add_image('backend/part1_read_throughput.png', 'Figure 2: Read Throughput vs. Failure Rate (Different Cluster Sizes)')
pdf.chapter_body(
    "Figure 2 Analysis: Similar to write throughput, read throughput is unaffected by cluster size. "
    "The decline with failure rate is slightly less steep than writes because read operations can sometimes be satisfied by the first available replica if consistency settings allow, "
    "though in this Majority setup (R=2), failures still cause latency penalties."
)

pdf.add_image('backend/part1_write_latency.png', 'Figure 3: Write Latency vs. Failure Rate (Different Cluster Sizes)')
pdf.chapter_body(
    "Figure 3 Analysis: Write latency shows a slight baseline increase for larger clusters due to the modeled network probability distribution (simulating larger network topology). "
    "Latency spikes significantly as failure rates increase, driven by the timeout mechanisms required when primary replicas are unavailable."
)

pdf.add_image('backend/part1_read_latency.png', 'Figure 4: Read Latency vs. Failure Rate (Different Cluster Sizes)')
pdf.chapter_body(
    "Figure 4 Analysis: Read latency follows the same trend as write latency. "
    "The stability of the curves across cluster sizes confirms that the system's lookup mechanism scales well. "
    "The primary driver of latency is clearly the node failure rate rather than the cluster size."
)

pdf.chapter_body(
    "Summary of Cluster Size Impact:\n"
    "The simulation results indicate that the system's performance is largely independent of the cluster size. Both read and write throughputs remain stable across cluster sizes of 100, 500, and 1000 nodes. "
    "This validates the scalability of the Consistent Hashing algorithm, which ensures that data distribution and routing overhead do not grow linearly with the number of nodes. "
    "The slight variations in latency are attributed to the modeled network probability distributions but do not show a systemic degradation as the cluster scales."
)

# 3.2 Impact of Replication Factor (N)
pdf.chapter_title('3.2 Impact of Replication Factor (N)')
pdf.chapter_body(
    "This section analyzes the impact of the replication factor (N) on performance.\n\n"
    "Experimental Setup:\n"
    "- Constants: Cluster Size (1000 nodes), Quorum Strategy (Majority: R=W=N/2 + 1).\n"
    "- Variables: Replication Factor N (10, 20, 50), Node Failure Rate (0% - 20%)."
)

pdf.add_image('backend/part2_write_throughput.png', 'Figure 5: Write Throughput vs. Failure Rate (Different N)')
pdf.chapter_body(
    "Figure 5 Analysis: Write throughput drops significantly as N increases. "
    "With N=50, the system must coordinate writes to 26 nodes (Majority) to succeed. "
    "This high coordination overhead results in lower throughput compared to N=10. "
    "Failure impact is also more pronounced at higher N because the probability of waiting for a slow or failed node increases with the quorum size."
)

pdf.add_image('backend/part2_read_throughput.png', 'Figure 6: Read Throughput vs. Failure Rate (Different N)')
pdf.chapter_body(
    "Figure 6 Analysis: Read throughput mirrors the write trend. "
    "Higher N requires gathering data (or checksums) from more nodes to satisfy the read quorum. "
    "The N=50 configuration shows the lowest throughput, highlighting the cost of high redundancy."
)

pdf.add_image('backend/part2_write_latency.png', 'Figure 7: Write Latency vs. Failure Rate (Different N)')
pdf.chapter_body(
    "Figure 7 Analysis: Write latency increases with N. "
    "The latency for N=50 is consistently higher than N=10. "
    "This is due to the 'slowest responder' effect: the operation is as slow as the slowest node in the required quorum. "
    "As the quorum size grows, the likelihood of including a slow node (tail latency) increases."
)

pdf.add_image('backend/part2_read_latency.png', 'Figure 8: Read Latency vs. Failure Rate (Different N)')
pdf.chapter_body(
    "Figure 8 Analysis: Read latency shows a similar pattern. "
    "The gap between N=10 and N=50 widens as the failure rate increases, demonstrating that maintaining consistency across a large replica set becomes increasingly expensive in unstable networks."
)

pdf.chapter_body(
    "Summary of Replication Factor Impact:\n"
    "The results clearly show an inverse relationship between the replication factor (N) and system performance. As N increases from 10 to 50, write throughput decreases significantly. "
    "This is expected, as the coordinator must replicate data to a larger number of nodes, incurring higher network and disk I/O overhead. "
    "Read latency also increases when using a Majority quorum strategy, as the system must wait for responses from a larger subset of nodes. "
    "However, this performance cost comes with the benefit of increased fault tolerance; a system with N=50 can survive significantly more simultaneous node failures than one with N=10."
)

# 3.3 Detailed Consistency Analysis
pdf.chapter_title('3.3 Detailed Consistency Analysis')
pdf.chapter_body(
    "This section provides a detailed breakdown of performance for specific replication factors (N=20, 50, 100) under different consistency levels (W/R).\n\n"
    "Experimental Setup:\n"
    "- Constants: Cluster Size (1000 nodes).\n"
    "- Variables: Consistency Level (W/R = 1, Majority, All), Node Failure Rate."
)

# N=20
pdf.chapter_body("Analysis for N=20:")
pdf.add_image('backend/graph_failure_impact_write_throughput_Cluster1000_N20.png', 'Figure 9: Write Throughput (N=20)')
pdf.chapter_body(
    "Figure 9 Analysis: For N=20, the W=1 configuration maintains high throughput even at 20% failure rate. "
    "In contrast, W=20 (All) crashes to near-zero throughput at just 5% failure rate, as the probability of all 20 nodes being alive drops drastically."
)

pdf.add_image('backend/graph_failure_impact_read_throughput_Cluster1000_N20.png', 'Figure 10: Read Throughput (N=20)')
pdf.chapter_body(
    "Figure 10 Analysis: Read throughput for R=1 is robust. "
    "The Majority quorum (R=11) shows a gradual decline, offering a balanced trade-off. "
    "Strict consistency (R=20) is unusable in failure-prone environments."
)

pdf.add_image('backend/graph_failure_impact_write_latency_Cluster1000_N20.png', 'Figure 11: Write Latency (N=20)')
pdf.chapter_body(
    "Figure 11 Analysis: Write latency for W=20 spikes immediately with any failures, dominated by timeout durations. "
    "W=1 latency remains flat and low."
)

pdf.add_image('backend/graph_failure_impact_read_latency_Cluster1000_N20.png', 'Figure 12: Read Latency (N=20)')
pdf.chapter_body(
    "Figure 12 Analysis: Read latency follows the write pattern. "
    "The Majority quorum latency increases moderately with failures but avoids the catastrophic spikes of the R=N configuration."
)

# N=50
pdf.chapter_body("Analysis for N=50:")
pdf.add_image('backend/graph_failure_impact_write_throughput_Cluster1000_N50.png', 'Figure 13: Write Throughput (N=50)')
pdf.chapter_body(
    "Figure 13 Analysis: With N=50, the trends from N=20 are amplified. "
    "The gap between W=1 and W=50 is massive. "
    "Even the Majority quorum (W=26) begins to show significant sensitivity to failure rates compared to the N=20 case."
)

pdf.add_image('backend/graph_failure_impact_read_throughput_Cluster1000_N50.png', 'Figure 14: Read Throughput (N=50)')
pdf.chapter_body(
    "Figure 14 Analysis: Read throughput for R=50 is practically zero if any failures exist. "
    "R=1 remains the only viable option for high-throughput requirements in this configuration."
)

pdf.add_image('backend/graph_failure_impact_write_latency_Cluster1000_N50.png', 'Figure 15: Write Latency (N=50)')
pdf.chapter_body(
    "Figure 15 Analysis: Latency for W=50 is consistently high. "
    "The Majority quorum latency is noticeably higher than in the N=20 case, reflecting the cost of coordinating 26 nodes."
)

pdf.add_image('backend/graph_failure_impact_read_latency_Cluster1000_N50.png', 'Figure 16: Read Latency (N=50)')
pdf.chapter_body(
    "Figure 16 Analysis: Read latency confirms that for large N, relaxed consistency (R=1 or small R) is essential for performance."
)

# N=100
pdf.chapter_body("Analysis for N=100:")
pdf.add_image('backend/graph_failure_impact_write_throughput_Cluster1000_N100.png', 'Figure 17: Write Throughput (N=100)')
pdf.chapter_body(
    "Figure 17 Analysis: At N=100, the system is extremely heavy. "
    "W=100 is purely theoretical and unusable in practice with any failures. "
    "Even W=1 shows lower absolute throughput than in N=20 due to the overhead of managing the large preference list."
)

pdf.add_image('backend/graph_failure_impact_read_throughput_Cluster1000_N100.png', 'Figure 18: Read Throughput (N=100)')
pdf.chapter_body(
    "Figure 18 Analysis: Read throughput for N=100 reinforces the scalability limit of strong consistency. "
    "Only eventual consistency (R=1) provides acceptable performance."
)

pdf.add_image('backend/graph_failure_impact_write_latency_Cluster1000_N100.png', 'Figure 19: Write Latency (N=100)')
pdf.chapter_body(
    "Figure 19 Analysis: Write latency is dominated by network variance and tail latencies of the 100-node group. "
    "The Majority quorum latency is significant."
)

pdf.add_image('backend/graph_failure_impact_read_latency_Cluster1000_N100.png', 'Figure 20: Read Latency (N=100)')
pdf.chapter_body(
    "Figure 20 Analysis: Read latency for N=100 shows that while the system can scale to 1000 nodes, the replication group size (N) should be kept smaller (e.g., N=3 to 5) for optimal latency, unless extreme fault tolerance is required."
)

pdf.chapter_body(
    "Summary of Consistency Levels:\n"
    "The detailed breakdown for N=20, 50, and 100 reveals the critical trade-off between consistency and availability (CAP Theorem):\n\n"
    "1. High Availability (W=1, R=1): These configurations exhibit the highest throughput and lowest latency. Crucially, they remain unaffected by node failure rates up to 20%, as the system only requires a single successful response.\n\n"
    "2. Strong Consistency (W=N, R=N): These configurations are extremely fragile. As seen in the graphs, even a small failure rate (5%) causes a catastrophic drop in throughput and a spike in latency, as operations time out waiting for unavailable nodes.\n\n"
    "3. Quorum Consistency (Majority): This offers a middle ground. Performance is lower than the W=1 case but significantly more robust than W=N. The system maintains operational stability as long as the number of failures does not exceed the minority of the replica set."
)

# 4. Conclusion
pdf.add_page()
pdf.chapter_title('4. Conclusion')
pdf.chapter_body(
    "The analysis confirms that the Dynamo-style architecture provides excellent availability and partition tolerance. "
    "By tuning N, R, and W, the system can be adapted for different workloads:\n"
    "- High Availability: Low R/W values (e.g., R=1, W=1).\n"
    "- Strong Consistency: High R/W values (e.g., R+W > N).\n\n"
    "The implementation of consistent hashing, vector clocks, and gossip protocols provides a robust foundation for a scalable "
    "distributed key-value store."
)

pdf.output('DynamoDB_Report.pdf', 'F')
print("PDF generated successfully.")

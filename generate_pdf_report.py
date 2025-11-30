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
        self.set_font('Times', 'B', 16)  # Increased font size for headings
        # No fill color, no rectangular box
        self.cell(0, 6, title, 0, 1, 'L')
        self.ln(4)

    def chapter_body(self, body):
        self.set_font('Times', '', 12)
        self.multi_cell(0, 5, body)
        self.ln()

    def add_image(self, image_path, caption):
        if os.path.exists(image_path):
            self.add_page()
            # Image first
            self.image(image_path, x=10, w=190)
            self.ln(2)
            # Caption below the image
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
    "even distribution of keys and load balancing. This allows nodes to be added or removed with minimal data movement.\n\n"
    "2.2 Replication (N, R, W)\n"
    "The system supports configurable replication factor (N) and quorum consistency levels (R for reads, W for writes). "
    "This allows users to tune the trade-off between availability and consistency.\n\n"
    "2.3 Vector Clocks\n"
    "To handle concurrent updates in a distributed environment, the system uses Vector Clocks. This mechanism tracks the causal "
    "ordering of events and detects conflicts that arise from network partitions or concurrent writes.\n\n"
    "2.4 Gossip Protocol\n"
    "Membership management and failure detection are handled via a Gossip protocol. Nodes periodically exchange state information "
    "to maintain an eventually consistent view of the cluster membership.\n\n"
    "2.5 Anti-Entropy (Merkle Trees)\n"
    "To synchronize replicas and repair inconsistencies, the system uses Merkle Trees. This allows nodes to efficiently compare "
    "large datasets and identify differences without exchanging the entire dataset."
)

# 3. Performance Analysis
pdf.chapter_title('3. Performance Analysis')
pdf.chapter_body(
    "A theoretical simulation was conducted to evaluate the system's performance under various conditions, specifically focusing on "
    "the impact of node failures and quorum configurations.\n\n"
    "The simulation modeled a 1,000-node cluster with varying replication factors (N=20, 50, 100) and failure rates (0% to 20%)."
)

# Add Images
pdf.add_image('backend/graph_failure_impact_write_throughput_Cluster1000_N50.png', 'Figure 1: Write Throughput vs. Failure Rate (N=50)')
pdf.chapter_body(
    "Figure 1 illustrates the impact of node failures on write throughput. Configurations with strict consistency (W=N) show a "
    "dramatic drop in throughput as failure rates increase, due to the high probability of at least one node being unavailable. "
    "Relaxed consistency (W=1) maintains high throughput."
)

pdf.add_image('backend/graph_failure_impact_read_throughput_Cluster1000_N50.png', 'Figure 2: Read Throughput vs. Failure Rate (N=50)')
pdf.chapter_body(
    "Figure 2 shows similar trends for read throughput. High availability configurations (R=1) are resilient to failures, while "
    "strong consistency configurations suffer significant performance degradation."
)

pdf.add_image('backend/graph_failure_impact_write_latency_Cluster1000_N50.png', 'Figure 3: Write Latency vs. Failure Rate (N=50)')
pdf.chapter_body(
    "Figure 3 demonstrates that write latency increases with the required quorum size. Under failure conditions, operations requiring "
    "responses from failed nodes time out, leading to high tail latency."
)

pdf.add_image('backend/graph_failure_impact_read_latency_Cluster1000_N50.png', 'Figure 4: Read Latency vs. Failure Rate (N=50)')
pdf.chapter_body(
    "Figure 4 confirms that read latency is minimized when R is small. The system can return the data from the fastest available replica, "
    "masking network jitter and node failures."
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

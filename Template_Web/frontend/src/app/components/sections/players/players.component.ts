import { Component, OnInit } from '@angular/core';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';
import jsPDF from 'jspdf';
import autoTable from 'jspdf-autotable';
@Component({
  selector: 'app-players',
  templateUrl: './players.component.html',
  styleUrls: ['./players.component.css']
})
export class PlayersComponent implements OnInit {
  // Properties for featured players
  featuredPlayers = [
    {
      name: 'Victor Osimhen',
      position: 'Forward',
      age: 24,
      country: 'Nigeria',
      club: 'Napoli',
      image: 'https://i.imgur.com/UhG18bB.jpg', // Using Imgur for better CORS support
      flag: 'https://i.imgur.com/FqOBQ2C.jpg', 
      matches: 32,
      goals: 26,
      assists: 5
    },
    {
      name: 'Achraf Hakimi',
      position: 'Right Back',
      age: 24,
      country: 'Morocco',
      club: 'Paris Saint-Germain',
      image: 'https://i.imgur.com/RQDJtQH.jpg',
      flag: 'https://i.imgur.com/YA5e7FB.jpg',
      matches: 39,
      goals: 5,
      assists: 6
    },
    {
      name: 'Sadio Mané',
      position: 'Forward',
      age: 31,
      country: 'Senegal',
      club: 'Al Nassr',
      image: 'https://i.imgur.com/HT3eEL8.jpg',
      flag: 'https://i.imgur.com/KoHWGV8.jpg',
      matches: 35,
      goals: 12,
      assists: 6
    }
  ];

  // Properties for player videos
 
  currentVideoIndex = 0;
  playerVideos = [
    {
      title: 'Victor Osimhen: Africa\'s Rising Star',
      thumbnail: 'https://i.ytimg.com/vi/u5iWk6DnrhM/hqdefault.jpg',
      videoUrl: 'https://www.youtube.com/embed/u5iWk6DnrhM'
    },
  {
  title: '10 Outstanding African Players That Have Won the UEFA Champions League',
  thumbnail: 'https://i.ytimg.com/vi/IILtMmukV_4/hqdefault.jpg',
  videoUrl: 'https://www.youtube.com/embed/IILtMmukV_4?start=36'
},
    {
      title: 'Mohammed Salah: Egyptian King Highlights',
      thumbnail: 'https://i.ytimg.com/vi/OafcIPFrjfU/hqdefault.jpg',
      videoUrl: 'https://www.youtube.com/embed/OafcIPFrjfU'
    }
  ];

  
  // Properties for rising talents (updated with CORS-friendly URLs)
  allTalents = [
    {
      name: 'Lamine Camara',
      age: 19,
      position: 'Midfielder',
      country: 'Senegal',
      image: 'assets/img/players/Lamine Camara.webp',
      potential: 5,
      type: 'midfielders'
    },
 {
      name: 'Karim Konaté',
      age: 19,
      position: 'Forward',
      country: 'Ivory Coast',
      image: 'assets/img/players/Karim Konate.jpeg',
      potential: 4,
      type: 'attackers'
    },
    {
      name: 'Bilal El Khannouss',
      age: 19,
      position: 'Attacking Midfielder',
      country: 'Morocco',
      image: 'assets/img/players/Bilal Khannous.webp',
      potential: 4,
      type: 'midfielders'
    },
    {
      name: 'Ibrahim Osman',
      age: 18,
      position: 'Winger',
      country: 'Ghana',
      image: 'assets/img/players/Ibrahim Osman.webp',
      potential: 4,
      type: 'attackers'
    },
    {
      name: 'Abdessamad Ezzalzouli',
      age: 21,
      position: 'Winger',
      country: 'Morocco',
      image: 'assets/img/players/Abdessamad Ezzalzouli.jpg',
      potential: 5,
      type: 'attackers'
    },
    {
      name: 'Chimuanya Ugochukwu',
      age: 20,
      position: 'Defensive Midfielder',
      country: 'Nigeria',
      image: 'assets/img/players/Chimuanya Ugochukwu.webp',
      potential: 4,
      type: 'midfielders'
    },
    {
      name: 'Amara Diouf',
      age: 17,
      position: 'Forward',
      country: 'Senegal',
      image: 'assets/img/players/Amara Diouf.webp',
      potential: 5,
      type: 'attackers'
    },
    {
      name: 'Evan Ndicka',
      age: 23,
      position: 'Defender',
      country: 'Ivory Coast',
      image: 'assets/img/players/Evan Ndicka.webp',
      potential: 4,
      type: 'defenders'
    }
  ];
  
  filteredTalents = this.allTalents;

  constructor(private sanitizer: DomSanitizer) { }

  ngOnInit(): void {
    // Add error handling for images
    this.setupImageErrorHandling();
  }

  // Method to safely embed YouTube videos
  getSafeUrl(url: string): SafeResourceUrl {
    return this.sanitizer.bypassSecurityTrustResourceUrl(url);
  }

  // Method to change the current video
  changeVideo(index: number): void {
    this.currentVideoIndex = index;
  }

  // Method to filter talents by position
  filterTalents(type: string): void {
    if (type === 'all') {
      this.filteredTalents = this.allTalents;
    } else {
      this.filteredTalents = this.allTalents.filter(talent => talent.type === type);
    }
  }
// Add to your players.component.ts file
handleImageError(event: any): void {
  // Use a default image when loading fails
  event.target.src = 'https://via.placeholder.com/300x400?text=Player+Image';
  // Remove height restriction to avoid stretched images
  event.target.style.height = 'auto';
}
  // Setup image error handling
  setupImageErrorHandling(): void {
    // Wait for DOM to be loaded
    setTimeout(() => {
      const images = document.querySelectorAll('.players-section img');
      images.forEach(img => {
        img.addEventListener('error', function() {
          // Replace with a default image if loading fails
          this.src = 'https://i.imgur.com/noimage.jpg';
          console.log('Image failed to load, replaced with default');
        });
      });
    }, 1000);
  }
/*PDF PART*/
// Advanced scout report generation with enhanced visuals and player-specific content
downloadScoutReport(player: any): void {
  // Create new PDF document with professional settings
  const doc = new jsPDF({
    orientation: 'portrait',
    unit: 'mm',
    format: 'a4'
  });
  const pageWidth = doc.internal.pageSize.width;
  
  // Professional header with branding
  doc.setFillColor(238, 30, 70); // #ee1e46
  doc.rect(0, 0, pageWidth, 30, 'F');
  doc.setTextColor(255);
  doc.setFontSize(22);
  doc.setFont('helvetica', 'bold');
  doc.text('TacticSense', 20, 15);
  doc.setFontSize(12);
  doc.setFont('helvetica', 'normal');
  doc.text('African Football Intelligence', 20, 22);
  doc.setFontSize(14);
  doc.setFont('helvetica', 'bold');
  doc.text('PROFESSIONAL SCOUT REPORT', pageWidth - 20, 15, {align: 'right'});
  
  // Player profile section with background
  doc.setDrawColor(240, 240, 240);
  doc.setFillColor(250, 250, 250);
  doc.roundedRect(10, 35, pageWidth - 20, 55, 3, 3, 'F');
  
  // Player name and basic info
  doc.setTextColor(0);
  doc.setFontSize(24);
  doc.setFont('helvetica', 'bold');
  doc.text(`${player.name}`, 20, 50);
  
  // Accent line
  doc.setDrawColor(238, 30, 70);
  doc.setLineWidth(0.5);
  doc.line(20, 53, 100, 53);
  
  // Player details
  doc.setFontSize(11);
  doc.setFont('helvetica', 'normal');
  doc.text(`Age: ${player.age} years`, 20, 62);
  doc.text(`Position: ${player.position}`, 20, 70);
  doc.text(`Nationality: ${player.country}`, 20, 78);
  
  // Potential rating visualization
  doc.setFontSize(11);
  doc.setFont('helvetica', 'bold');
  doc.text('Potential Rating:', 120, 62);
  
  // Draw star rating
  for (let i = 0; i < 5; i++) {
    if (i < player.potential) {
      doc.setFillColor(238, 30, 70);
    } else {
      doc.setFillColor(200, 200, 200);
    }
    doc.circle(123 + (i * 8), 68, 3, 'F');
  }
  
  // Player Type/Role
  doc.setFont('helvetica', 'bold');
  doc.text('Player Type:', 120, 78);
  doc.setFont('helvetica', 'normal');
  
  // Set role based on player type and position
  let playerRole = '';
  if (player.type === 'attackers') {
    if (player.position.includes('Winger')) {
      playerRole = 'Dynamic Winger';
    } else if (player.position.includes('Forward')) {
      playerRole = 'Modern Center Forward';
    } else {
      playerRole = 'Versatile Attacker';
    }
  } else if (player.type === 'midfielders') {
    if (player.position.includes('Defensive')) {
      playerRole = 'Defensive Anchor';
    } else if (player.position.includes('Attacking')) {
      playerRole = 'Creative Playmaker';
    } else {
      playerRole = 'Box-to-Box Midfielder';
    }
  } else {
    playerRole = 'Ball-Playing Defender';
  }
  
  doc.text(playerRole, 158, 78);
  
  // Executive summary box
  doc.setDrawColor(238, 30, 70);
  doc.setFillColor(252, 240, 242);
  doc.roundedRect(10, 95, pageWidth - 20, 35, 3, 3, 'F');
  doc.setFontSize(13);
  doc.setFont('helvetica', 'bold');
  doc.setTextColor(238, 30, 70);
  doc.text('EXECUTIVE SUMMARY', 20, 105);
  doc.setTextColor(60, 60, 60);
  doc.setFontSize(10);
  doc.setFont('helvetica', 'normal');
  
  // Player-specific executive summaries
  let executiveSummary = '';
  
  // Player-specific content
  switch(player.name) {
    case 'Karim Konaté':
      executiveSummary = 'Elite young striker prospect with exceptional finishing ability and physical prowess. Shows significant potential to become a top-tier forward in Europe within 2-3 seasons. Strong aerial ability and intelligent movement in the final third make him a complete forward prospect.';
      break;
      
    case 'Lamine Camara':
      executiveSummary = 'Outstanding midfield talent with exceptional technical ability and vision. His 5-star potential rating reflects his capacity to develop into an elite central midfielder. Shows maturity beyond his years in his decision-making and spatial awareness.';
      break;
      
    case 'Bilal El Khannouss':
      executiveSummary = 'Creative attacking midfielder with remarkable vision and technical skills. His ability to operate in tight spaces and find progressive passing lanes marks him as a high-potential playmaker. Expected to develop into a top-level number 10 in European football.';
      break;
      
    case 'Ibrahim Osman':
      executiveSummary = 'Explosive young winger with electric pace and direct dribbling ability. At just 18, shows significant promise with his decision-making in the final third improving rapidly. Has the potential to develop into a dangerous wide attacker at Champions League level.';
      break;
      
    case 'Abdessamad Ezzalzouli':
      executiveSummary = 'Elite winger prospect with outstanding technical ability and creativity. His 5-star potential reflects his ceiling as a potential world-class wide player. Combines excellent close control with intelligent movement and increasingly refined end product.';
      break;
      
    case 'Chimuanya Ugochukwu':
      executiveSummary = 'Physically imposing defensive midfielder with excellent tactical awareness and technical security. Reads the game exceptionally well for his age and offers both defensive stability and progressive passing from deep positions.';
      break;
      
    case 'Amara Diouf':
      executiveSummary = 'Exceptional forward talent with precocious technical ability and game intelligence. At just 17, his 5-star potential indicates a possible trajectory toward elite level status. His maturity in decision-making belies his young age.';
      break;
      
    case 'Evan Ndicka':
      executiveSummary = 'Modern ball-playing defender with excellent physical attributes and tactical intelligence. Already performing at a high level, he combines defensive solidity with progressive passing ability and aerial dominance.';
      break;
      
    default:
      executiveSummary = `Young ${player.position.toLowerCase()} showing promising development and solid technical foundation. With appropriate development, could become a valuable squad player at higher levels within 1-2 seasons.`;
  }
  
  const execLines = doc.splitTextToSize(executiveSummary, pageWidth - 40);
  doc.text(execLines, 20, 112);
  
  // Technical assessment section
  doc.setFontSize(13);
  doc.setFont('helvetica', 'bold');
  doc.setTextColor(0);
  doc.text('Technical Assessment', 20, 140);
  
  // Custom technical assessment for each player
  let technicalText = '';
  
  switch(player.name) {
    case 'Karim Konaté':
      technicalText = 'Konaté demonstrates exceptional finishing ability with both feet, particularly excelling at first-time strikes and composure in one-on-one situations. His shooting technique is refined, with clean ball-striking and good accuracy. While his hold-up play is developing, he already shows good awareness in link-up situations. Areas for development include his creative passing range and first touch under intense pressure.';
      break;
      
    case 'Lamine Camara':
      technicalText = 'Camara possesses exceptional passing range and vision, consistently breaking lines with his distribution. His first touch is excellent, allowing him to receive under pressure and maintain possession. Shows impressive technique in set-piece delivery and striking from distance. His close control in tight spaces stands out, though his weaker foot usage can still improve.';
      break;
      
    case 'Bilal El Khannouss':
      technicalText = 'El Khannouss displays remarkable technical ability in tight spaces with close control and dribbling technique that allows him to escape pressure efficiently. His creative passing and vision are elite for his age group. Shows excellent weight of pass and ability to execute in the final third. Can further refine his shooting technique to increase goal threat.';
      break;
      
    case 'Ibrahim Osman':
      technicalText = 'Osman demonstrates excellent close control at speed and the ability to beat defenders in one-on-one situations. His crossing technique is developing well, with improved accuracy and decision-making. Finishing ability is promising though inconsistent, with clear potential for improvement as he matures physically.';
      break;
      
    case 'Abdessamad Ezzalzouli':
      technicalText = 'Ezzalzouli shows exceptional technical ability in his ball manipulation, first touch, and dribbling capacity. Can beat defenders with both trickery and pace, making him unpredictable in attacking situations. His shooting technique is clean, though shot selection can improve. Crossing is above average and continues to develop.';
      break;
      
    case 'Chimuanya Ugochukwu':
      technicalText = 'Ugochukwu possesses impressive technical security for a defensive midfielder of his physical profile. His first touch is consistently clean, and his passing range shows variety and precision. Ball-carrying ability is good, using his physical attributes to protect possession effectively. Could develop more creative passing options in the final third.';
      break;
      
    case 'Amara Diouf':
      technicalText = 'Despite his young age, Diouf displays advanced technical ability with excellent close control and dribbling skills. His first touch is exceptional, creating space in tight areas. Shows composure in finishing situations beyond his years. Creative passing ability is developing rapidly, with good vision for line-breaking opportunities.';
      break;
      
    case 'Evan Ndicka':
      technicalText = 'Ndicka demonstrates strong technical ability for a central defender, with clean first touches and composed passing under pressure. Shows good technique in long-range distribution and progressive passes into midfield. Comfortable carrying the ball forward and initiating attacks. Set-piece threat with good heading technique.';
      break;
      
    default:
      technicalText = `Shows solid technical fundamentals appropriate for a ${player.position.toLowerCase()}. Main technical strengths include ball control and basic passing. Areas for continued technical development focus on consistency and execution under pressure.`;
  }
  
  const techLines = doc.splitTextToSize(technicalText, pageWidth - 40);
  doc.setFontSize(10);
  doc.setFont('helvetica', 'normal');
  doc.text(techLines, 20, 148);
  
  // Physical assessment
  let yPos = 148 + techLines.length * 4.5;
  doc.setFontSize(13);
  doc.setFont('helvetica', 'bold');
  doc.text('Physical Profile', 20, yPos);
  
  // Custom physical assessment for each player
  let physicalText = '';
  
  switch(player.name) {
    case 'Karim Konaté':
      physicalText = 'Standing at 1.83m with a strong build, Konaté combines good pace with impressive strength. His acceleration over short distances is particularly noteworthy, allowing him to exploit small spaces in the penalty area. Shows good jumping ability and timing in aerial duels. Endurance levels are good, maintaining performance throughout 90 minutes.';
      break;
      
    case 'Lamine Camara':
      physicalText = 'Athletic midfielder with good balance and agility. While not imposing in stature, he uses his low center of gravity effectively to shield the ball and resist challenges. Shows impressive stamina, covering significant ground consistently. Acceleration is good, though top-end speed is merely average for his position.';
      break;
      
    case 'Bilal El Khannouss':
      physicalText = 'Slight build but demonstrates excellent balance and agility, allowing him to evade challenges effectively. Not physically dominant but intelligent in using his body position to protect the ball. Stamina is good and improving as he matures physically. Will benefit from continued physical development programs.';
      break;
      
    case 'Ibrahim Osman':
      physicalText = 'Extremely quick player with outstanding acceleration and good top speed. Still developing physically at 18, with room to add strength without compromising his agility. Shows good stamina but can fade in the latter stages of matches, an area that should improve with physical maturity.';
      break;
      
    case 'Abdessamad Ezzalzouli':
      physicalText = 'Well-balanced physical profile combining good pace with increasingly functional strength. Maintains speed while dribbling, a valuable asset for his position. Agility and change of direction are excellent. Stamina has improved significantly over the past season, now able to maintain high-intensity actions throughout matches.';
      break;
      
    case 'Chimuanya Ugochukwu':
      physicalText = 'Imposing physical presence with excellent strength and aerial ability. Good mobility for his size, covering ground efficiently in defensive actions. Stamina is excellent, maintaining performance levels consistently. Uses his frame effectively in duels and to shield possession.';
      break;
      
    case 'Amara Diouf':
      physicalText = 'Still physically developing at 17, but already shows promising athletic traits. Good acceleration and agility with balanced movement patterns. Not physically imposing yet, but uses intelligent body positioning to compensate. Significant physical development expected in coming years as he matures.';
      break;
      
    case 'Evan Ndicka':
      physicalText = 'Excellent physical specimen with ideal attributes for a modern defender. Standing at 1.90m with athletic build, combines strength with good mobility. Strong in aerial duels and physical contests. Recovery pace is good for his height, allowing him to defend larger areas effectively.';
      break;
      
    default:
      physicalText = 'Physical development is appropriate for age, with potential for further improvement as physical maturity increases. Current athletic profile supports role requirements with room for continued enhancement.';
  }
  
  yPos += 8;
  const physLines = doc.splitTextToSize(physicalText, pageWidth - 40);
  doc.setFontSize(10);
  doc.setFont('helvetica', 'normal');
  doc.text(physLines, 20, yPos);
  
  // Check if we need a new page before attribute graph
  yPos += physLines.length * 4.5 + 10;
  if (yPos > 250) {
    doc.addPage();
    yPos = 30;
  }
  
  // Attribute graph
  doc.setFontSize(13);
  doc.setFont('helvetica', 'bold');
  doc.text('Key Attributes', 20, yPos);
  yPos += 10;
  
  // Player-specific attributes with precise ratings
  const attributes = [];
  
  switch(player.name) {
    case 'Karim Konaté':
      attributes.push(
        {name: 'Finishing', rating: 85},
        {name: 'Physical Power', rating: 82},
        {name: 'Pace', rating: 79},
        {name: 'Aerial Ability', rating: 75},
        {name: 'Movement', rating: 81},
        {name: 'Ball Control', rating: 73}
      );
      break;
      
    case 'Lamine Camara':
      attributes.push(
        {name: 'Passing', rating: 86},
        {name: 'Vision', rating: 83},
        {name: 'Ball Control', rating: 84},
        {name: 'Tactical IQ', rating: 82},
        {name: 'Work Rate', rating: 80},
        {name: 'Long Shots', rating: 76}
      );
      break;
      
    case 'Bilal El Khannouss':
      attributes.push(
        {name: 'Technical Skill', rating: 84},
        {name: 'Creative Passing', rating: 83},
        {name: 'Close Control', rating: 85},
        {name: 'Vision', rating: 82},
        {name: 'Decision Making', rating: 78},
        {name: 'Finishing', rating: 71}
      );
      break;
      
    case 'Ibrahim Osman':
      attributes.push(
        {name: 'Acceleration', rating: 86},
        {name: 'Dribbling', rating: 81},
        {name: 'Crossing', rating: 75},
        {name: 'Pace', rating: 87},
        {name: 'Agility', rating: 83},
        {name: 'Finishing', rating: 74}
      );
      break;
      
    case 'Abdessamad Ezzalzouli':
      attributes.push(
        {name: 'Dribbling', rating: 87},
        {name: 'Technical Skill', rating: 84},
        {name: 'Pace', rating: 83},
        {name: 'Creativity', rating: 82},
        {name: 'Flair', rating: 85},
        {name: 'Finishing', rating: 75}
      );
      break;
      
    case 'Chimuanya Ugochukwu':
      attributes.push(
        {name: 'Tackling', rating: 80},
        {name: 'Positioning', rating: 83},
        {name: 'Physical Strength', rating: 85},
        {name: 'Passing', rating: 78},
        {name: 'Interceptions', rating: 82},
        {name: 'Aerial Ability', rating: 79}
      );
      break;
      
    case 'Amara Diouf':
      attributes.push(
        {name: 'Technical Skill', rating: 84},
        {name: 'Dribbling', rating: 83},
        {name: 'Creativity', rating: 82},
        {name: 'Finishing', rating: 80},
        {name: 'Pace', rating: 78},
        {name: 'Decision Making', rating: 76}
      );
      break;
      
    case 'Evan Ndicka':
      attributes.push(
        {name: 'Aerial Ability', rating: 84},
        {name: 'Tackling', rating: 82},
        {name: 'Positioning', rating: 83},
        {name: 'Strength', rating: 85},
        {name: 'Passing', rating: 79},
        {name: 'Concentration', rating: 80}
      );
      break;
      
    default:
      if (player.type === 'attackers') {
        attributes.push(
          {name: 'Finishing', rating: 75},
          {name: 'Movement', rating: 73},
          {name: 'Pace', rating: 78},
          {name: 'Technical Skill', rating: 74},
          {name: 'Creativity', rating: 70}
        );
      } else if (player.type === 'midfielders') {
        attributes.push(
          {name: 'Passing', rating: 76},
          {name: 'Vision', rating: 74},
          {name: 'Ball Control', rating: 75},
          {name: 'Tactical IQ', rating: 72},
          {name: 'Work Rate', rating: 77}
        );
      } else {
        attributes.push(
          {name: 'Tackling', rating: 76},
          {name: 'Positioning', rating: 75},
          {name: 'Aerial Ability', rating: 78},
          {name: 'Strength', rating: 77},
          {name: 'Concentration', rating: 74}
        );
      }
  }
  
  // Draw attribute bars
  const barMaxWidth = 120;
  const barHeight = 7;
  const attributeX = 30;
  let attributeY = yPos;
  
  for (const attr of attributes) {
    // Label
    doc.setFontSize(10);
    doc.setFont('helvetica', 'normal');
    doc.text(attr.name, attributeX, attributeY);
    
    // Background bar
    doc.setFillColor(240, 240, 240);
    doc.rect(attributeX + 50, attributeY - 5, barMaxWidth, barHeight, 'F');
    
    // Value bar
    const valueWidth = (barMaxWidth * attr.rating) / 100;
    doc.setFillColor(238, 30, 70);
    doc.rect(attributeX + 50, attributeY - 5, valueWidth, barHeight, 'F');
    
    // Value text
    doc.setTextColor(0);
    doc.text(attr.rating.toString(), attributeX + 50 + valueWidth + 5, attributeY);
    
    attributeY += 12;
  }
  
  // Add a new page for stats and recommendations
  doc.addPage();
  
// Performance data header
doc.setFontSize(14);
doc.setFont('helvetica', 'bold');
doc.text('Performance Data', 20, 20);
doc.setFontSize(9);
doc.setFont('helvetica', 'italic');
doc.text('Statistics from last competitive season | Percentile ranking vs positional peers', 20, 28);
// Performance statistics table based on player position
let tableData = [];
let tableHeaders = [];

 // Player-specific performance data
if (player.name === 'Karim Konaté') {
  tableHeaders = [['Metric', 'Value', 'Percentile', 'Rating']];
  tableData = [
    ['Goals per 90', '0.68', '92%', 'Elite'],
    ['xG per 90', '0.61', '87%', 'Very Good'],
    ['Shots per 90', '3.2', '81%', 'Very Good'],
    ['Shot Accuracy', '48%', '79%', 'Good'],
    ['Conversion Rate', '21%', '88%', 'Very Good'],
    ['Succ. Dribbles', '2.1', '76%', 'Good'],
    ['Aerial Duels Won', '3.9', '82%', 'Very Good'],
    ['Progressive Runs', '2.4', '74%', 'Good']
  ];
} else if (player.name === 'Lamine Camara') {
    tableHeaders = [['Metric', 'Value', 'Percentile', 'Rating']];
    tableData = [
      ['Pass Accuracy', '89%', '91%', 'Elite'],
      ['Prog. Passes', '8.4', '88%', 'Very Good'],
      ['Key Passes', '2.3', '85%', 'Very Good'],
      ['Ball Recoveries', '7.6', '83%', 'Very Good'],
      ['Tackles + Interceptions', '4.2', '76%', 'Good'],
      ['Progressive Carries', '3.8', '82%', 'Very Good'],
      ['Succ. Take-ons', '1.9', '77%', 'Good']
    ];
  } else if (player.type === 'attackers') {
    tableHeaders = [['Metric', 'Value', 'Percentile', 'Rating']];
    tableData = [
      ['Goals per 90', '0.42', '74%', 'Good'],
      ['xG per 90', '0.38', '70%', 'Good'],
      ['Shots per 90', '2.3', '68%', 'Above Average'],
      ['Shot Accuracy', '41%', '66%', 'Above Average'],
      ['Succ. Dribbles', '2.0', '72%', 'Good'],
      ['Progressive Runs', '2.7', '75%', 'Good']
    ];
  } else if (player.type === 'midfielders') {
    tableHeaders = [['Metric', 'Value', 'Percentile', 'Rating']];
    tableData = [
      ['Pass Accuracy', '87%', '84%', 'Very Good'],
      ['Prog. Passes', '6.2', '79%', 'Good'],
      ['Key Passes', '1.8', '72%', 'Good'],
      ['Ball Recoveries', '6.2', '81%', 'Very Good'],
      ['Tackles + Interceptions', '3.8', '74%', 'Good']
    ];
  } else {
    tableHeaders = [['Metric', 'Value', 'Percentile', 'Rating']];
    tableData = [
      ['Aerial Duels Won', '3.1', '82%', 'Very Good'],
      ['Tackles', '2.3', '74%', 'Good'],
      ['Interceptions', '1.9', '76%', 'Good'],
      ['Clearances', '4.2', '85%', 'Very Good'],
      ['Pass Accuracy', '86%', '71%', 'Good']
    ];
  }
  let tableEnd = 150; 
  // Draw table with autoTable
  const result = autoTable(doc, {
    head: tableHeaders,
    body: tableData,
    startY: 32,
    theme: 'striped',
    headStyles: { 
      fillColor: [238, 30, 70],
      textColor: [255, 255, 255],
      fontStyle: 'bold'
    },
    columnStyles: {
      0: {cellWidth: 40},
      1: {cellWidth: 30},
      2: {cellWidth: 30},
      3: {cellWidth: 40}
    },
    alternateRowStyles: {fillColor: [252, 240, 242]},
    margin: {left: 20, right: 20}
  });
  
// Draw table with autoTable using the didDrawPage callback
autoTable(doc, {
  head: tableHeaders,
  body: tableData,
  startY: 32,
  theme: 'striped',
  headStyles: { 
    fillColor: [238, 30, 70],
    textColor: [255, 255, 255],
    fontStyle: 'bold'
  },
  columnStyles: {
    0: {cellWidth: 40},
    1: {cellWidth: 30},
    2: {cellWidth: 30},
    3: {cellWidth: 40}
  },
  alternateRowStyles: {fillColor: [252, 240, 242]},
  margin: {left: 20, right: 20},
  // Add this callback to properly capture the final Y position
  didDrawPage: (data) => {
    tableEnd = data.cursor.y + 15;
  }
});

// Development recommendations section
doc.setFontSize(14);
doc.setFont('helvetica', 'bold');
doc.text('Development Recommendations', 20, tableEnd);

// Now tableEnd can be used without any TypeScript errors
doc.setFontSize(14);
doc.setFont('helvetica', 'bold');
doc.text('Development Recommendations', 20, tableEnd);
  // Development recommendations
  doc.setFontSize(14);
  doc.setFont('helvetica', 'bold');
  doc.text('Development Recommendations', 20, tableEnd);
  
  // Player-specific development recommendations
  let recommendations = '';
  
  switch(player.name) {
    case 'Karim Konaté':
      recommendations = `
1. Technical Focus: Work on refining first touch in tight spaces and improving linkup play with midfielders
2. Tactical Development: Further exposure to different tactical systems to improve versatility
3. Physical Development: Continue lower body strength program to improve holding off defenders
4. Decision-making: Video analysis sessions focusing on decision-making in the final third
5. Competitive Environment: Regular minutes against high-level opposition to accelerate development curve
      `;
      break;
      
    case 'Lamine Camara':
      recommendations = `
1. Technical Focus: Develop weaker foot to improve all-round passing capability
2. Tactical Development: Experience in different midfield roles to increase positional versatility
3. Physical Development: Focused work on explosive power to improve defensive transitions
4. Mental Aspect: Leadership training to maximize on-field influence and communication
5. Match Exposure: Regular competitive minutes at highest possible level to accelerate development
      `;
      break;
      
    case 'Bilal El Khannouss':
      recommendations = `
1. Technical Focus: Continue developing shooting technique and composure in finishing situations
2. Tactical Development: Experience in multiple attacking midfield positions (central and wide)
3. Physical Development: Core and lower body strength program to improve durability
4. Decision-making: Work on final third decision-making and risk assessment
5. Competitive Exposure: Regular minutes against tactically sophisticated opponents
      `;
      break;
      
    case 'Ibrahim Osman':
      recommendations = `
1. Technical Focus: Improve crossing accuracy and variety; develop finishing composure
2. Tactical Development: Understanding of defensive responsibilities in different systems
3. Physical Development: Structured strength program to add functional muscle mass
4. Decision-making: Video analysis of final third choices and timing
5. Competition Level: Exposure to high-level opposition to accelerate tactical development
      `;
      break;
      
    case 'Abdessamad Ezzalzouli':
      recommendations = `
1. Technical Focus: Refine end product (crossing and shooting) consistency
2. Tactical Development: Experience in multiple attacking systems and positions
3. Physical Development: Continue building functional strength without losing agility
4. Decision-making: Improve efficiency in final third actions and risk assessment
5. Competitive Level: Regular exposure to Champions League level competition
      `;
      break;
      
    case 'Chimuanya Ugochukwu':
      recommendations = `
1. Technical Focus: Develop more variety in forward passing range
2. Tactical Development: Experience in both single and double pivot systems
3. Physical Maintenance: Continue current physical program while focusing on injury prevention
4. Game Intelligence: Video analysis sessions on positional adjustments and defensive scanning
5. Competitive Exposure: Regular minutes at highest possible level in competitive league
      `;
      break;
      
    case 'Amara Diouf':
      recommendations = `
1. Technical Focus: Refine finishing technique from varied situations
2. Tactical Development: Gradual exposure to different tactical systems appropriate for age
3. Physical Development: Carefully structured program focusing on natural development
4. Age-appropriate Approach: Balanced development without excessive pressure given his young age
5. Mental Support: Strong psychological guidance and mentorship through development pathway
      `;
      break;
      
    case 'Evan Ndicka':
      recommendations = `
1. Technical Focus: Continue developing progressive passing range and confidence
2. Tactical Development: Experience in both back-three and back-four systems
3. Physical Maintenance: Focus on mobility maintenance alongside strength work
4. Leadership Development: Communication and organizational skill enhancement
5. Competition Level: Regular exposure to highest level European competition
      `;
      break;
      
    default:
      recommendations = `
1. Technical Focus: Improvement needed in core positional skills
2. Tactical Development: Exposure to positional responsibilities within different systems
3. Physical Development: Customized program focusing on key athletic components
4. Competitive Environment: Regular minutes at appropriate competitive level
5. Mentorship: Pairing with experienced professional in same position recommended
      `;
  }
  
  const recLines = doc.splitTextToSize(recommendations.trim(), pageWidth - 40);
  doc.setFontSize(10);
  doc.setFont('helvetica', 'normal');
  doc.text(recLines, 20, tableEnd + 10);
  
  // Conclusion section
  const recEnd = tableEnd + 10 + recLines.length * 5;
  
  doc.setDrawColor(238, 30, 70);
  doc.setFillColor(252, 240, 242);
  doc.roundedRect(10, recEnd, pageWidth - 20, 25, 3, 3, 'F');
  doc.setFontSize(14);
  doc.setFont('helvetica', 'bold');
  doc.setTextColor(238, 30, 70);
  doc.text('SCOUT CONCLUSION', 20, recEnd + 10);
  
  doc.setTextColor(60, 60, 60);
  doc.setFontSize(10);
  doc.setFont('helvetica', 'normal');
  
  // Player-specific conclusions
  let conclusion = '';
  
  switch(player.name) {
    case 'Karim Konaté':
      conclusion = 'HIGH PRIORITY TARGET. Konaté projects as a potential elite-level striker with significant ROI potential. Recommended for immediate recruitment consideration. Player profile aligns strongly with organizational recruitment strategy for attacking talent.';
      break;
      
    case 'Lamine Camara':
      conclusion = 'PRIORITY TARGET. Camara demonstrates exceptional potential as a central midfielder with both defensive and offensive capabilities. His 5-star potential rating is justified by his technical security and tactical intelligence. Recommended for active recruitment.';
      break;
      
    case 'Bilal El Khannouss':
      conclusion = 'PRIORITY TARGET. El Khannouss shows elite potential as a creative midfielder with excellent technical fundamentals and vision. Recommended for active scouting and potential acquisition based on continued development.';
      break;
      
    case 'Ibrahim Osman':
      conclusion = 'MONITORING TARGET. Osman shows significant promise as a dynamic winger with excellent physical attributes. Recommend continued close monitoring to assess development trajectory over next 6-12 months before making acquisition decision.';
      break;
      
    case 'Abdessamad Ezzalzouli':
      conclusion = 'HIGH PRIORITY TARGET. Ezzalzouli demonstrates elite potential as a creative winger capable of operating at Champions League level. Recommended for immediate consideration with significant long-term value proposition.';
      break;
      
    case 'Chimuanya Ugochukwu':
      conclusion = 'PRIORITY TARGET. Ugochukwu projects as a high-level defensive midfielder with excellent physical and technical attributes. Recommended for active recruitment consideration with potential for significant ROI.';
      break;
      
    case 'Amara Diouf':
      conclusion = 'DEVELOPMENTAL TARGET. At 17, Diouf shows exceptional promise with 5-star potential but requires careful development. Recommended for long-term recruitment strategy with development pathway clearly defined.';
      break;
      
    case 'Evan Ndicka':
      conclusion = 'IMMEDIATE TARGET. Ndicka is already performing at a high level with potential to develop further. Represents excellent value proposition with immediate contribution capability and resale value.';
      break;
      
    default:
      conclusion = `MONITORING LIST. ${player.name} shows promising attributes that warrant continued scouting attention. Recommend reassessment in 6 months to evaluate development progression.`;
  }
  
  const conLines = doc.splitTextToSize(conclusion, pageWidth - 40);
  doc.text(conLines, 20, recEnd + 20);
  
  // Add watermark to all pages
  const pageCount = doc.getNumberOfPages();
  for(let i = 1; i <= pageCount; i++) {
    doc.setPage(i);
    
    // Footer
    doc.setFontSize(8);
    doc.setTextColor(150);
    doc.setFont('helvetica', 'normal');
    doc.text('CONFIDENTIAL - TacticSense African Talent ID', 20, doc.internal.pageSize.height - 10);
    doc.text('© TacticSense ' + new Date().getFullYear(), pageWidth/2, doc.internal.pageSize.height - 10, {align: 'center'});
    doc.text('Page ' + i + ' of ' + pageCount, pageWidth - 20, doc.internal.pageSize.height - 10, {align: 'right'});
    
  
  }
  
  // Save the PDF with a professional filename
  doc.save(`TacticSense_Scout_Report_${player.name.replace(/\s+/g, '_')}.pdf`);
}
  
}
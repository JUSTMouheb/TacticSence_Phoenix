import { Component, OnInit } from '@angular/core';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';


declare var bootstrap: any; // For Bootstrap modal

@Component({
  selector: 'app-clubs',
  templateUrl: './clubs.component.html',
  styleUrls: ['./clubs.component.css']
})
export class ClubsComponent implements OnInit {
  
    selectedClub: any = null;
  sanitizedWebsiteUrl: SafeResourceUrl;


  // Featured clubs
  featuredClubs = [
    {
      name: 'Al Ahly SC',
      country: 'Egypt',
      countryFlag: 'assets/img/flags/egypt.png',
      badge: 'assets/img/clubs/al-ahly.png',
      founded: 1907,
      leagueTitles: 43,
      continentalTitles: 11,
      stadium: 'Al Ahly Stadium',
      website: 'https://alahlyegypt.com/fr',
      stadiumCapacity: '50,000',
      history: 'Al Ahly Sporting Club, founded in 1907, is the most successful club in African football history. Based in Cairo, Egypt, the club has dominated both domestic and continental competitions, earning the nickname "Club of the Century" from CAF. Al Ahly has consistently produced top talent and maintained its status as a powerhouse of African football.',
      honours: [
        'CAF Champions League: 11 titles (1982, 1987, 2001, 2005, 2006, 2008, 2012, 2013, 2020, 2021, 2023)',
        'CAF Super Cup: 8 titles',
        'Egyptian Premier League: 43 titles',
        'Egypt Cup: 37 titles',
        'FIFA Club World Cup: Bronze Medal (2006, 2020, 2021)'
      ],
      keyPlayers: [
        { name: 'Mohamed El Shenawy', position: 'Goalkeeper', image: 'assets/img/players/el-shenawy.jpg' },
        { name: 'Hussein El Shahat', position: 'Winger', image: 'assets/img/players/el-shahat.jpg' },
        { name: 'Percy Tau', position: 'Forward', image: 'assets/img/players/percy-tau.jpg' }
      ]
    },
    {
      name: 'Wydad AC',
      country: 'Morocco',
      countryFlag: 'assets/img/flags/morocco.png',
      badge: 'assets/img/clubs/wydad.png',
      founded: 1937,
      leagueTitles: 22,
      continentalTitles: 3,
      stadium: 'Stade Mohammed V',
      website:'https://wydad.net/en/',
      stadiumCapacity: '45,000',
      history: 'Wydad Athletic Club, founded in 1937, is one of Morocco\'s and Africa\'s most prestigious football clubs. Based in Casablanca, Wydad has established itself as a continental heavyweight with multiple CAF Champions League titles. Known for their passionate supporters and the electric atmosphere at Stade Mohammed V, Wydad continues to be a dominant force in North African football.',
      honours: [
        'CAF Champions League: 3 titles (1992, 2017, 2022)',
        'CAF Super Cup: 1 title (2018)',
        'Moroccan League: 22 titles',
        'Moroccan Throne Cup: 9 titles'
      ],
      keyPlayers: [
        { name: 'Yahya Jabrane', position: 'Midfielder', image: 'assets/img/players/jabrane.jpg' },
        { name: 'Zouhair El Moutaraji', position: 'Forward', image: 'assets/img/players/el-moutaraji.jpg' },
        { name: 'Ayoub El Amloud', position: 'Defender', image: 'assets/img/players/el-amloud.jpg' }
      ]
    },
    {
      name: 'Mamelodi Sundowns',
      country: 'South Africa',
      countryFlag: 'assets/img/flags/south-africa.png',
      badge: 'assets/img/clubs/sundowns.png',
      founded: 1970,
      leagueTitles: 14,
      continentalTitles: 1,
      stadium: 'Loftus Versfeld Stadium',
      stadiumCapacity: '51,762',
       website:'https://sundownsfc.co.za/',
      history: 'Mamelodi Sundowns Football Club, founded in 1970, has emerged as South Africa\'s most successful club in the Premier Soccer League era. Under the ownership of mining magnate Patrice Motsepe, Sundowns transformed into a continental powerhouse, winning the CAF Champions League in 2016. Known for their attractive, technical style of play, they\'ve dominated domestic competitions in recent years.',
      honours: [
        'CAF Champions League: 1 title (2016)',
        'CAF Super Cup: 1 title (2017)',
        'South African Premier League: 14 titles',
        'Nedbank Cup: 6 titles',
        'MTN 8: 4 titles',
        'Telkom Knockout: 3 titles'
      ],
      keyPlayers: [
        { name: 'Ronwen Williams', position: 'Goalkeeper', image: 'assets/img/players/williams.jpg' },
        { name: 'Themba Zwane', position: 'Midfielder', image: 'assets/img/players/zwane.jpg' },
        { name: 'Peter Shalulile', position: 'Forward', image: 'assets/img/players/shalulile.jpg' }
      ]
    }
  ];

  // Latest news
  latestNews = [
    {
      title: 'Al Ahly secured record-extending 12th CAF Champions League title',
      date: 'May 5, 2025',
      category: 'Champions League',
      image: 'assets/img/news/ahly-champions.webp',
      excerpt: 'The Egyptian giants defeated South African side Mamelodi Sundowns 3-1 on aggregate in the final to claim their 12th continental crown, further cementing their status as Africa\'s most successful club.',
      url:'https://www.bbc.com/sport/football/articles/c722wj52e24o#:~:text=Egyptian%20giants%20Al%20Ahly%20won,of%20the%20final%20in%20Cairo.'
    },
    {
      title: 'Raja Casablanca announces major stadium renovation project',
      date: 'April 28, 2025',
      category: 'Infrastructure',
      image: 'assets/img/news/stadium-renovation.jpg',
      excerpt: 'The Moroccan powerhouse has unveiled plans for a comprehensive €80 million renovation of their historic stadium, aiming to create one of Africa\'s most modern sporting venues by 2027.',
      url:'https://www.myjoyonline.com/mohammed-v-sports-complex-in-casablanca-unveils-new-look/'
    },
    {
      title: 'TP Mazembe signs promising Zambian midfielder in record deal',
      date: 'April 23, 2025',
      category: 'Transfers',
      image: 'assets/img/news/mazembe-transfer.jpg',
      excerpt: 'The Congolese club has completed the signing of 20-year-old Zambian international midfielder Kings Kangwa from Red Bull Salzburg for a reported fee of €3.5 million, breaking the record for the most expensive intra-African transfer.',
      url:'https://english.ahram.org.eg/NewsContent/6/54/2685/Sports/Africa/Mazembe-land-Zambia%E2%80%99s-Kalaba-and-Kakonje.aspx'
    },
    {
      title: 'CAF announces expanded club competitions format for 2026',
      date: 'April 15, 2025',
      category: 'CAF Competitions',
      image: 'assets/img/news/caf-announcement.png',
      excerpt: 'The Confederation of African Football has revealed a revamped format for continental club competitions starting in 2026, featuring more teams, increased prize money, and a new league phase similar to UEFA\'s recent changes.',
      url:'https://en.africatopsports.com/2025/02/18/caf-sets-to-revamp-interclub-competitions/'
    }
  ];



  // Rising clubs
  risingClubs = [
    {
      name: 'Pyramids FC',
      country: 'Egypt',
      image: 'assets/img/clubs/pyramids-stadium.jpeg',
      badge: 'assets/img/clubs/pyramids-badge.jpeg',
      description: 'Founded in 2008 and backed by significant investment since 2018, Pyramids FC has rapidly risen to challenge the traditional Egyptian powerhouses.'
    },
    {
      name: 'RS Berkane',
      country: 'Morocco',
      image: 'assets/img/clubs/berkane-stadium.jpeg',
      badge: 'assets/img/clubs/berkane-badge.jpeg',
      description: 'Recent winners of the CAF Confederation Cup, the Orange Boys have established themselves as serious contenders both domestically and continentally.'
    },
    {
      name: 'Simba SC',
      country: 'Tanzania',
      image: 'assets/img/clubs/simba-stadium.jpeg',
      badge: 'assets/img/clubs/simba-badge.png',
      description: 'With massive investment and rapidly growing support, Simba has transformed into East Africa\'s football powerhouse with continental ambitions.'
    },
    {
      name: 'Petro de Luanda',
      country: 'Angola',
      image: 'assets/img/clubs/petro-stadium.jpeg',
      badge: 'assets/img/clubs/petro-badge.png',
      description: 'Recent CAF Champions League semifinalists, the oil-backed club has invested heavily to become Angola\'s flagbearer in African competitions.'
    }
  ];

  // Top academies
  topAcademies = [
    {
      name: 'ASEC Mimosas Academy',
      country: 'Ivory Coast',
      logo: 'assets/img/clubs/asec-logo.png',
      image: 'assets/img/academies/asec-academy.jpeg',
      description: 'Founded in 1994, ASEC Mimosas\' famed "Académie MimoSifcom" is arguably Africa\'s most successful football academy, having produced dozens of players for Europe\'s top leagues and the Ivorian national team.',
      graduates: [
        'Yaya Touré (Manchester City, Barcelona)',
        'Kolo Touré (Arsenal, Manchester City)',
        'Gervinho (Arsenal, Roma)',
        'Salomon Kalou (Chelsea)',
        'Emmanuel Eboué (Arsenal)'
      ]
    },
    {
      name: 'Ajax Cape Town',
      country: 'South Africa',
      logo: 'assets/img/clubs/ajax-cape-town-logo.png',
      image: 'assets/img/academies/ajax-cape-town-academy.jpeg',
      description: 'A partnership with Dutch giants Ajax Amsterdam, this academy implements the famous Ajax training methodology adapted to African talents, focusing on technical skills and tactical understanding.',
      graduates: [
        'Steven Pienaar (Everton, Tottenham)',
        'Benni McCarthy (Porto, Blackburn)',
        'Thulani Serero (Ajax Amsterdam)',
        'Eyong Enoh (Ajax Amsterdam)'
      ]
    },
    {
      name: 'Right to Dream Academy',
      country: 'Ghana',
      logo: 'assets/img/clubs/right-to-dream-logo.png',
      image: 'assets/img/academies/right-to-dream-academy.jpeg',
      description: 'Founded in 1999, this non-profit academy combines education with high-quality football training, providing pathways to professional careers and university scholarships abroad.',
      graduates: [
        'Mohammed Kudus (West Ham)',
        'Kamaldeen Sulemana (Southampton)',
        'Abdul Majeed Waris (FC Porto)',
        'Thomas Partey (Arsenal)'
      ]
    },
    {
      name: 'Diambars FC',
      country: 'Senegal',
      logo: 'assets/img/clubs/diambars-logo.jpeg',
      image: 'assets/img/academies/diambars-academy.jpeg',
      description: 'Co-founded by Patrick Vieira, Diambars combines education and football training with the mission to develop responsible citizens and professional footballers from Senegal.',
      graduates: [
        'Idrissa Gueye (Everton)',
        'Kara Mbodj (Anderlecht)',
        'Pape Souaré (Crystal Palace)',
        'Saliou Ciss (Nancy)'
      ]
    }
  ];

  selectedAcademy = 0; // Default open academy
 
  clubDetailsModal: any; // Bootstrap modal reference

  constructor(private sanitizer: DomSanitizer) { }

  ngOnInit(): void {
    // Initialize modal when component loads
    document.addEventListener('DOMContentLoaded', () => {
      const modalEl = document.getElementById('clubDetailsModal');
      if (modalEl) {
        this.clubDetailsModal = new bootstrap.Modal(modalEl);
      }
    });
  }

  // Show club details when hovering over map marker
  showClubDetails(clubId: number): void {
    // Find the club by ID and show tooltip or highlight related content
    console.log(`Showing details for club ID: ${clubId}`);
    // You could implement a tooltip or highlight the corresponding club card
  }

  // Select academy to expand
  selectAcademy(index: number): void {
    this.selectedAcademy = this.selectedAcademy === index ? -1 : index;
  }

  // Open club details modal
  openClubDetails(club: any): void {
    this.selectedClub = club;
    // Sanitize the URL for security when using in iframes
    this.sanitizedWebsiteUrl = this.sanitizer.bypassSecurityTrustResourceUrl(club.website);
    // Wait for Angular to update the DOM before showing modal
    setTimeout(() => {
      if (this.clubDetailsModal) {
        this.clubDetailsModal.show();
      } else {
        const modalEl = document.getElementById('clubDetailsModal');
        if (modalEl) {
          this.clubDetailsModal = new bootstrap.Modal(modalEl);
          this.clubDetailsModal.show();
        }
      }
    }, 100);
  }



}
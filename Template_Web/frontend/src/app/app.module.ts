import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { FormsModule } from '@angular/forms'; // Add this import
import { HttpClientModule } from '@angular/common/http'; // Add this import
import { AppComponent } from './app.component';
import { HeaderComponent } from './components/shared/header/header.component';
import { FooterComponent } from './components/shared/footer/footer.component';
import { HomeComponent } from './components/sections/home/home.component';
import { AboutComponent } from './components/sections/about/about.component';
import { PlayersComponent } from './components/sections/players/players.component';
import { ClubsComponent } from './components/sections/clubs/clubs.component';
import { AnalyticsComponent } from './components/sections/analytics/analytics.component';
import { ContactComponent } from './components/sections/contact/contact.component';
import { CsvService } from './services/CsvService';
import { LoginComponent } from './components/shared/login/login.component';
import { SocialAuthCallbackComponent } from './components/auth/social-auth-callback/social-auth-callback.component';

@NgModule({
  declarations: [
    AppComponent,
    HeaderComponent,
    FooterComponent,
    HomeComponent,
    AboutComponent,
    PlayersComponent,
    ClubsComponent,
    AnalyticsComponent,
    ContactComponent,
    LoginComponent,
    SocialAuthCallbackComponent,
    
  ],
  imports: [
    BrowserModule,
    FormsModule,
    HttpClientModule // Add this module to your imports array
   
  ],
  providers: [ CsvService],
  bootstrap: [AppComponent]
})
export class AppModule { }
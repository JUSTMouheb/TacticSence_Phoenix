import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';

import { AppComponent } from './app.component';
import { HeaderComponent } from './components/shared/header/header.component';
import { FooterComponent } from './components/shared/footer/footer.component';
import { HomeComponent } from './components/sections/home/home.component';
import { AboutComponent } from './components/sections/about/about.component';
import { PlayersComponent } from './components/sections/players/players.component';
import { ClubsComponent } from './components/sections/clubs/clubs.component';
import { AnalyticsComponent } from './components/sections/analytics/analytics.component';
import { ContactComponent } from './components/sections/contact/contact.component';

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
    ContactComponent
  ],
  imports: [
    BrowserModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
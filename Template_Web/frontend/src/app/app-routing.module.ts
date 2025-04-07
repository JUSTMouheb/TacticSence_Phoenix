import { NgModule } from '@angular/core';
import {Routes } from '@angular/router';

import { HomeComponent } from './components/sections/home/home.component';
import { AboutComponent } from './components/sections/about/about.component';
import { PlayersComponent } from './components/sections/players/players.component';
import { ClubsComponent } from './components/sections/clubs/clubs.component';
import { AnalyticsComponent } from './components/sections/analytics/analytics.component';
import { ContactComponent } from './components/sections/contact/contact.component';

const routes: Routes = [
  { path: '', component: HomeComponent },
  { path: 'about', component: AboutComponent },
  { path: 'players', component: PlayersComponent },
  { path: 'clubs', component: ClubsComponent },
  { path: 'analytics', component: AnalyticsComponent },
  { path: 'contact', component: ContactComponent },
  { path: '**', redirectTo: '' }
];

@NgModule({
  
})
export class AppRoutingModule { }
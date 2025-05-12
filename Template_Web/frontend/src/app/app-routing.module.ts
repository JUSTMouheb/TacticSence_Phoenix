import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

import { HomeComponent } from './components/sections/home/home.component';
import { AboutComponent } from './components/sections/about/about.component';
import { PlayersComponent } from './components/sections/players/players.component';
import { ClubsComponent } from './components/sections/clubs/clubs.component';
//import { AnalyticsComponent } from './components/sections/analytics/analytics.component';
import { ContactComponent } from './components/sections/contact/contact.component';
import { SocialAuthCallbackComponent } from './components/auth/social-auth-callback/social-auth-callback.component';

const routes: Routes = [
  { path: '', component: HomeComponent },
  { path: 'about', component: AboutComponent },
  { path: 'players', component: PlayersComponent },
  { path: 'clubs', component: ClubsComponent },
  //{ path: 'analytics', component: AnalyticsComponent },
  { path: 'contact', component: ContactComponent },
  { path: '**', redirectTo: '' }, // Add comma here
  {
    path: 'auth/facebook-callback',
    component: SocialAuthCallbackComponent,
    data: { provider: 'facebook' }
  },
  {
    path: 'auth/google-callback',
    component: SocialAuthCallbackComponent,
    data: { provider: 'google' }
  },
  {
    path: 'auth/linkedin-callback',
    component: SocialAuthCallbackComponent,
    data: { provider: 'linkedin' }
  }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
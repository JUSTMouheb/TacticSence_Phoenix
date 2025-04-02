import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { DashboardComponent } from './components/dashboard/dashboard.component';
import { MatchesComponent } from './components/matches/matches.component';
import { PlayersComponent } from './components/players/players.component';
import { StatisticsComponent } from './components/statistics/statistics.component';
import { TeamsComponent } from './components/teams/teams.component';

const routes: Routes = [
  { path: '', redirectTo: '/dashboard', pathMatch: 'full' },
  { path: 'dashboard', component: DashboardComponent },
  { path: 'matches', component: MatchesComponent },
  { path: 'players', component: PlayersComponent },
  { path: 'statistics', component: StatisticsComponent },
  { path: 'teams', component: TeamsComponent },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }